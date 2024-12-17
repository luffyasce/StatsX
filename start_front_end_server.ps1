$pythonScripts = @("data_server.py", "chart_server.py", "live_server.py")

$projectRoot = "C:\Users\Daniel\projects\StatsX\front_end\server"
$condaEnv = "StatsX"

foreach ($script in $pythonScripts) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command & {
        cd $projectRoot
        conda activate $condaEnv
        python $script
    }"
}
