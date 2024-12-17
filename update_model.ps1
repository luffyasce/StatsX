$pythonScripts = @("update_hist_model.py")

$projectRoot = "C:\Users\Daniel\projects\StatsX"
$condaEnv = "statsX"

foreach ($script in $pythonScripts) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command & {
        cd $projectRoot
        conda activate $condaEnv
        python $script
    }"
}
