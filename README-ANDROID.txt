MAKE THE REAL ANDROID APP (no terminal, free, ~5 minutes)
The game is bundled inside the app (works offline, no browser bars, own icon). Only the multiplayer
server address is typed in the app (Online screen) - the install page shows it with a Copy button.

1. Make a free account on github.com and create a new repository (any name, Private is fine).
2. In the repository click "Add file > Upload files" and drag in EVERYTHING inside this android-build folder
   (package.json, capacitor.config.json, the www folder).
3. The workflow file is in a hidden folder. If .github did not upload: click "Add file > Create new file",
   type the name  .github/workflows/build-apk.yml  and paste the contents of build-apk.yml from this folder. Commit.
4. Open the "Actions" tab > "Build Android APK" > "Run workflow". Wait ~5-8 minutes until it is green.
5. Open the finished run, download the artifact "crownlands-apk", unzip it, rename app-debug.apk to crownlands.apk
6. Put crownlands.apk into the  public  folder of the game server. The "Download Android app" button now appears
   on the install page. On the phone: tap it, open the file, allow "install unknown apps" when asked.
To update the app after changing the game: replace android-build\www\index.html, upload again, run the workflow again.
