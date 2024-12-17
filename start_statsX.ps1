#$pythonScripts = @("start_ctp_md.py", "start_account_record.py", "live_model_order_tracker.py", "live_model_order_archive.py", "live_model_md_assess.py", "live_model_option_targets.py")
$pythonScripts = @("start_ctp_md.py", "live_model_order_tracker.py", "live_model_order_archive.py", "live_model_md_assess.py", "live_model_option_targets.py", "live_model_oi_vol_assess.py")

$projectRoot = "C:\Users\Daniel\projects\StatsX"
$condaEnv = "StatsX"

foreach ($script in $pythonScripts) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command & {
        cd $projectRoot
        conda activate $condaEnv
        python $script
    }"
}