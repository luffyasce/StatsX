$pythonScripts = @("download_hist_data_czce.py", "download_hist_data_dce.py", "download_hist_data_gfex.py", "download_hist_data_shfe.py", "download_hist_data_misc.py", "pretreat_and_process_hist_data.py")

$projectRoot = "C:\Users\Daniel\projects\StatsX\x_data_proc_entry"
$condaEnv = "StatsX"

foreach ($script in $pythonScripts) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command & {
        cd $projectRoot
        conda activate $condaEnv
        python $script
    }"
}
