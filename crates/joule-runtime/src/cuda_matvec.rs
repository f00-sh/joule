//! CUDA driver-API matvec (ADR 0003).
//!
//! Uses dynamic `libcuda.so` only — no static toolkit link. Loads a tiny PTX
//! kernel and runs y = A·x for n×n f32 matrices on device 0 when available.

use libloading::Library;
use std::ffi::c_void;
use std::sync::OnceLock;

type CUresult = i32;
type CUdevice = i32;
type CUcontext = *mut c_void;
type CUdeviceptr = u64;
type CUmodule = *mut c_void;
type CUfunction = *mut c_void;

const CUDA_SUCCESS: CUresult = 0;

/// Minimal PTX: one thread computes full n=4 matvec (matches MATMUL_DIM lab size).
const MATVEC4_PTX: &str = r#"
.version 6.0
.target sm_50
.address_size 64

.visible .entry joule_matvec4(
    .param .u64 param_A,
    .param .u64 param_x,
    .param .u64 param_y
)
{
    .reg .u64 %A, %xp, %yp, %addr;
    .reg .f32 %acc, %a, %xv;
    .reg .u32 %i, %j, %tmp;
    .reg .pred %p;

    ld.param.u64 %A, [param_A];
    ld.param.u64 %xp, [param_x];
    ld.param.u64 %yp, [param_y];

    mov.u32 %i, 0;
ROW:
    setp.ge.u32 %p, %i, 4;
    @%p bra DONE;
    mov.f32 %acc, 0f00000000;
    mov.u32 %j, 0;
COL:
    setp.ge.u32 %p, %j, 4;
    @%p bra STORE;
    // A[i*4+j]
    mul.lo.u32 %tmp, %i, 4;
    add.u32 %tmp, %tmp, %j;
    mul.lo.u32 %tmp, %tmp, 4;
    cvt.u64.u32 %addr, %tmp;
    add.u64 %addr, %A, %addr;
    ld.global.f32 %a, [%addr];
    // x[j]
    mul.lo.u32 %tmp, %j, 4;
    cvt.u64.u32 %addr, %tmp;
    add.u64 %addr, %xp, %addr;
    ld.global.f32 %xv, [%addr];
    fma.rn.f32 %acc, %a, %xv, %acc;
    add.u32 %j, %j, 1;
    bra COL;
STORE:
    mul.lo.u32 %tmp, %i, 4;
    cvt.u64.u32 %addr, %tmp;
    add.u64 %addr, %yp, %addr;
    st.global.f32 [%addr], %acc;
    add.u32 %i, %i, 1;
    bra ROW;
DONE:
    ret;
}
"#;

struct CudaApi {
    _lib: Library,
    cu_init: unsafe extern "C" fn(u32) -> CUresult,
    cu_device_get: unsafe extern "C" fn(*mut CUdevice, i32) -> CUresult,
    cu_ctx_create: unsafe extern "C" fn(*mut CUcontext, u32, CUdevice) -> CUresult,
    cu_ctx_destroy: unsafe extern "C" fn(CUcontext) -> CUresult,
    cu_mem_alloc: unsafe extern "C" fn(*mut CUdeviceptr, usize) -> CUresult,
    cu_mem_free: unsafe extern "C" fn(CUdeviceptr) -> CUresult,
    cu_memcpy_htod: unsafe extern "C" fn(CUdeviceptr, *const c_void, usize) -> CUresult,
    cu_memcpy_dtoh: unsafe extern "C" fn(*mut c_void, CUdeviceptr, usize) -> CUresult,
    cu_module_load_data: unsafe extern "C" fn(*mut CUmodule, *const c_void) -> CUresult,
    cu_module_get_function: unsafe extern "C" fn(*mut CUfunction, CUmodule, *const i8) -> CUresult,
    cu_launch_kernel: unsafe extern "C" fn(
        CUfunction,
        u32,
        u32,
        u32,
        u32,
        u32,
        u32,
        u32,
        *mut c_void,
        *mut *mut c_void,
        *mut *mut c_void,
    ) -> CUresult,
    cu_ctx_synchronize: unsafe extern "C" fn() -> CUresult,
    cu_module_unload: unsafe extern "C" fn(CUmodule) -> CUresult,
}

fn load_api() -> Result<&'static CudaApi, String> {
    static API: OnceLock<Result<CudaApi, String>> = OnceLock::new();
    match API.get_or_init(|| {
        let candidates = [
            "libcuda.so.1",
            "libcuda.so",
            "/usr/lib/libcuda.so.1",
            "/usr/lib64/libcuda.so.1",
        ];
        let mut lib = None;
        for c in candidates {
            if let Ok(l) = unsafe { Library::new(c) } {
                lib = Some(l);
                break;
            }
        }
        let lib = lib.ok_or_else(|| "libcuda not found".to_string())?;
        unsafe {
            let api = CudaApi {
                cu_init: *lib.get(b"cuInit\0").map_err(|e| format!("cuInit: {e}"))?,
                cu_device_get: *lib
                    .get(b"cuDeviceGet\0")
                    .map_err(|e| format!("cuDeviceGet: {e}"))?,
                cu_ctx_create: *lib
                    .get(b"cuCtxCreate_v2\0")
                    .or_else(|_| lib.get(b"cuCtxCreate\0"))
                    .map_err(|e| format!("cuCtxCreate: {e}"))?,
                cu_ctx_destroy: *lib
                    .get(b"cuCtxDestroy_v2\0")
                    .or_else(|_| lib.get(b"cuCtxDestroy\0"))
                    .map_err(|e| format!("cuCtxDestroy: {e}"))?,
                cu_mem_alloc: *lib
                    .get(b"cuMemAlloc_v2\0")
                    .or_else(|_| lib.get(b"cuMemAlloc\0"))
                    .map_err(|e| format!("cuMemAlloc: {e}"))?,
                cu_mem_free: *lib
                    .get(b"cuMemFree_v2\0")
                    .or_else(|_| lib.get(b"cuMemFree\0"))
                    .map_err(|e| format!("cuMemFree: {e}"))?,
                cu_memcpy_htod: *lib
                    .get(b"cuMemcpyHtoD_v2\0")
                    .or_else(|_| lib.get(b"cuMemcpyHtoD\0"))
                    .map_err(|e| format!("cuMemcpyHtoD: {e}"))?,
                cu_memcpy_dtoh: *lib
                    .get(b"cuMemcpyDtoH_v2\0")
                    .or_else(|_| lib.get(b"cuMemcpyDtoH\0"))
                    .map_err(|e| format!("cuMemcpyDtoH: {e}"))?,
                cu_module_load_data: *lib
                    .get(b"cuModuleLoadData\0")
                    .map_err(|e| format!("cuModuleLoadData: {e}"))?,
                cu_module_get_function: *lib
                    .get(b"cuModuleGetFunction\0")
                    .map_err(|e| format!("cuModuleGetFunction: {e}"))?,
                cu_launch_kernel: *lib
                    .get(b"cuLaunchKernel\0")
                    .map_err(|e| format!("cuLaunchKernel: {e}"))?,
                cu_ctx_synchronize: *lib
                    .get(b"cuCtxSynchronize\0")
                    .map_err(|e| format!("cuCtxSynchronize: {e}"))?,
                cu_module_unload: *lib
                    .get(b"cuModuleUnload\0")
                    .map_err(|e| format!("cuModuleUnload: {e}"))?,
                _lib: lib,
            };
            Ok(api)
        }
    }) {
        Ok(api) => Ok(api),
        Err(e) => Err(e.clone()),
    }
}

fn check(rc: CUresult, what: &str) -> Result<(), String> {
    if rc == CUDA_SUCCESS {
        Ok(())
    } else {
        Err(format!("{what} failed: cuda error {rc}"))
    }
}

/// Host reference matvec y = A·x for n×n row-major A.
pub fn host_matvec_f32(a: &[f32], x: &[f32], n: usize) -> Result<Vec<f32>, String> {
    if a.len() < n * n || x.len() < n {
        return Err("host_matvec: short buffers".into());
    }
    let mut y = vec![0.0f32; n];
    for (row, slot) in y.iter_mut().enumerate().take(n) {
        let mut acc = 0.0f32;
        let base = row * n;
        for col in 0..n {
            acc += a[base + col] * x[col];
        }
        *slot = acc;
    }
    Ok(y)
}

/// Run y = A·x on the GPU for **n=4** via CUDA driver API + PTX (real device compute).
///
/// Fails if libcuda / device / kernel launch is unavailable — not a silent host fallback.
pub fn cuda_matvec4_f32(a: &[f32; 16], x: &[f32; 4]) -> Result<[f32; 4], String> {
    let api = load_api()?;
    unsafe {
        check((api.cu_init)(0), "cuInit")?;
        let mut dev: CUdevice = 0;
        check((api.cu_device_get)(&mut dev, 0), "cuDeviceGet")?;
        let mut ctx: CUcontext = std::ptr::null_mut();
        check((api.cu_ctx_create)(&mut ctx, 0, dev), "cuCtxCreate")?;

        let mut d_a: CUdeviceptr = 0;
        let mut d_x: CUdeviceptr = 0;
        let mut d_y: CUdeviceptr = 0;
        let bytes_a = 16 * 4;
        let bytes_x = 4 * 4;
        let cleanup = |api: &CudaApi,
                       ctx: CUcontext,
                       d_a: CUdeviceptr,
                       d_x: CUdeviceptr,
                       d_y: CUdeviceptr,
                       module: CUmodule| {
            if !module.is_null() {
                let _ = (api.cu_module_unload)(module);
            }
            if d_a != 0 {
                let _ = (api.cu_mem_free)(d_a);
            }
            if d_x != 0 {
                let _ = (api.cu_mem_free)(d_x);
            }
            if d_y != 0 {
                let _ = (api.cu_mem_free)(d_y);
            }
            if !ctx.is_null() {
                let _ = (api.cu_ctx_destroy)(ctx);
            }
        };

        if let Err(e) = check((api.cu_mem_alloc)(&mut d_a, bytes_a), "cuMemAlloc A") {
            cleanup(api, ctx, 0, 0, 0, std::ptr::null_mut());
            return Err(e);
        }
        if let Err(e) = check((api.cu_mem_alloc)(&mut d_x, bytes_x), "cuMemAlloc x") {
            cleanup(api, ctx, d_a, 0, 0, std::ptr::null_mut());
            return Err(e);
        }
        if let Err(e) = check((api.cu_mem_alloc)(&mut d_y, bytes_x), "cuMemAlloc y") {
            cleanup(api, ctx, d_a, d_x, 0, std::ptr::null_mut());
            return Err(e);
        }
        if let Err(e) = check(
            (api.cu_memcpy_htod)(d_a, a.as_ptr() as *const c_void, bytes_a),
            "HtoD A",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, std::ptr::null_mut());
            return Err(e);
        }
        if let Err(e) = check(
            (api.cu_memcpy_htod)(d_x, x.as_ptr() as *const c_void, bytes_x),
            "HtoD x",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, std::ptr::null_mut());
            return Err(e);
        }

        let mut module: CUmodule = std::ptr::null_mut();
        // PTX must be NUL-terminated for cuModuleLoadData.
        let mut ptx = MATVEC4_PTX.as_bytes().to_vec();
        ptx.push(0);
        if let Err(e) = check(
            (api.cu_module_load_data)(&mut module, ptx.as_ptr() as *const c_void),
            "cuModuleLoadData",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, std::ptr::null_mut());
            return Err(e);
        }
        let mut func: CUfunction = std::ptr::null_mut();
        let name = b"joule_matvec4\0";
        if let Err(e) = check(
            (api.cu_module_get_function)(&mut func, module, name.as_ptr() as *const i8),
            "cuModuleGetFunction",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, module);
            return Err(e);
        }

        let mut p_a = d_a;
        let mut p_x = d_x;
        let mut p_y = d_y;
        let mut args: [*mut c_void; 3] = [
            &mut p_a as *mut _ as *mut c_void,
            &mut p_x as *mut _ as *mut c_void,
            &mut p_y as *mut _ as *mut c_void,
        ];
        if let Err(e) = check(
            (api.cu_launch_kernel)(
                func,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                std::ptr::null_mut(),
                args.as_mut_ptr(),
                std::ptr::null_mut(),
            ),
            "cuLaunchKernel",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, module);
            return Err(e);
        }
        if let Err(e) = check((api.cu_ctx_synchronize)(), "cuCtxSynchronize") {
            cleanup(api, ctx, d_a, d_x, d_y, module);
            return Err(e);
        }

        let mut y = [0.0f32; 4];
        if let Err(e) = check(
            (api.cu_memcpy_dtoh)(y.as_mut_ptr() as *mut c_void, d_y, bytes_x),
            "DtoH y",
        ) {
            cleanup(api, ctx, d_a, d_x, d_y, module);
            return Err(e);
        }
        cleanup(api, ctx, d_a, d_x, d_y, module);
        Ok(y)
    }
}

/// Production stage helper: matvec on weight matrix via CUDA when possible.
/// Returns (y, used_cuda).
pub fn production_matvec4(a: &[f32; 16], x: &[f32; 4]) -> Result<([f32; 4], bool), String> {
    match cuda_matvec4_f32(a, x) {
        Ok(y) => Ok((y, true)),
        Err(_cuda_err) => {
            // Host path only when CUDA genuinely unavailable (CI without GPU).
            let host = host_matvec_f32(a, x, 4)?;
            let mut y = [0.0f32; 4];
            y.copy_from_slice(&host[..4]);
            Ok((y, false))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cuda_or_host_matvec_matches_reference() {
        let a = [
            1.0, 0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, 4.0,
        ];
        let x = [1.0, 1.0, 1.0, 1.0];
        let (y, used_cuda) = production_matvec4(&a, &x).expect("matvec");
        let expect = host_matvec_f32(&a, &x, 4).unwrap();
        for i in 0..4 {
            assert!(
                (y[i] - expect[i]).abs() < 1e-4,
                "y[{i}]={} expect={}",
                y[i],
                expect[i]
            );
        }
        // On this NVIDIA host, prefer real CUDA path.
        if crate::gpu_engine::probe_cuda_devices().available {
            let gpu = cuda_matvec4_f32(&a, &x);
            assert!(
                gpu.is_ok(),
                "CUDA matvec must work when probe available: {gpu:?}"
            );
            let gy = gpu.unwrap();
            for i in 0..4 {
                assert!((gy[i] - expect[i]).abs() < 1e-3, "gpu y[{i}]={}", gy[i]);
            }
            eprintln!("OBSERVE cuda-matvec: used_cuda={used_cuda} y={y:?} gpu_ok=true");
        } else {
            eprintln!("OBSERVE cuda-matvec: no device; host fallback y={y:?}");
        }
    }
}
