; Inno Setup 6 — native Windows GUI installer for joule
; Built on GitHub Actions (windows-latest) via packaging/windows/build-native.ps1
;
; Output: joule-{version}-windows-x86_64-setup.exe

#ifndef MyAppVersion
  #define MyAppVersion "0.0.0"
#endif
#ifndef MyAppArch
  #define MyAppArch "x86_64"
#endif
#ifndef MyBinPath
  #define MyBinPath "joule.exe"
#endif
#ifndef MyOutDir
  #define MyOutDir "dist"
#endif
#ifndef MySourceRoot
  #define MySourceRoot ".."
#endif

#define MyAppName "joule"
#define MyAppPublisher "f00-sh"
#define MyAppURL "https://joule.f00.sh/"
#define MyAppExeName "joule.exe"

[Setup]
; Stable product GUID (do not change across releases)
AppId={{8F3C2A1B-9E4D-4B7A-A6C1-5D2E8F0B1A73}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL=https://github.com/f00-sh/joule/releases
DefaultDirName={autopf}\joule
DefaultGroupName=joule
DisableProgramGroupPage=no
SourceDir={#MySourceRoot}
LicenseFile=LICENSE
InfoBeforeFile=packaging\windows\INSTALL-INFO.txt
OutputDir={#MyOutDir}
OutputBaseFilename=joule-{#MyAppVersion}-windows-{#MyAppArch}-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayIcon={app}\{#MyAppExeName}
UninstallDisplayName=joule
VersionInfoVersion={#MyAppVersion}.0
VersionInfoCompany={#MyAppPublisher}
VersionInfoDescription=joule — donate idle compute, open-weight AI pool
VersionInfoProductName=joule
CloseApplications=yes
RestartApplications=no
ChangesEnvironment=yes
AllowNoIcons=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional icons:"; Flags: unchecked
Name: "addpath"; Description: "Add joule to user &PATH (recommended for CLI)"; GroupDescription: "PATH:"; Flags: checkedonce

[Files]
Source: "{#MyBinPath}"; DestDir: "{app}"; DestName: "joule.exe"; Flags: ignoreversion
Source: "README.md"; DestDir: "{app}"; Flags: ignoreversion isreadme
Source: "LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "CHANGELOG.md"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist
Source: "packaging\windows\README.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "man\joule.1.md"; DestDir: "{app}\man"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
Name: "{group}\joule"; Filename: "{app}\{#MyAppExeName}"; Comment: "Open joule pool dashboard"
Name: "{group}\joule CLI help"; Filename: "{cmd}"; Parameters: "/K ""{app}\{#MyAppExeName}"" --help"; Comment: "CLI help in terminal"
Name: "{group}\Uninstall joule"; Filename: "{uninstallexe}"
Name: "{autodesktop}\joule"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon; Comment: "Open joule pool dashboard"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch joule GUI now"; Flags: nowait postinstall skipifsilent

[Registry]
Root: HKCU; Subkey: "Environment"; ValueType: expandsz; ValueName: "Path"; \
  ValueData: "{olddata};{app}"; Tasks: addpath; Check: NeedsAddPath(ExpandConstant('{app}'))

[Code]
function NeedsAddPath(Param: string): Boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, 'Environment', 'Path', OrigPath) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Param + ';', ';' + OrigPath + ';') = 0;
end;
