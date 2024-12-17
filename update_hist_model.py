from datetime import datetime, timedelta
from model.historical.sub.informed_broker import InformedBroker
from model.historical.sub.term_structure import TermStructure
import model.historical.sub.iv_position as model_ivp
import model.historical.sub.price_position as model_pricep
from model.historical.valid_broker_position_analyse import ValidBrokerPosAnalyse
from model.historical.informed_broker_position_composition import IBPosComp
from output.model_outputs.informed_broker_consistency import IBConsistency
from output.model_outputs.total_IB_pos_structure import TotIBPosStructure
from output.model_outputs.oi_targets import OiTargets
from utils.tool.datetime_wrangle import yield_dates
from utils.tool.configer import Config
from utils.tool.decorator import try_catch
from utils.tool.base_class import APSchedulerBase

exchange_list = Config().exchange_list


def sub_models(date: datetime = datetime.now()):

    # PAST AND CURRENT TRADING DAYS MODEL BASIC DATA

    for e in exchange_list:
        ts = TermStructure(date, e)
        r = ts.analyse_term_structure()
        ts.save_term_structure(r)

    for e in exchange_list:
        ib = InformedBroker(date, e)
        res = ib.calc_corr_by_contract()
        ib.save_pos_corr_by_contract(res)
        res = ib.select_informed_broker_by_contract()
        if res:
            ib.save_informed_broker_info_by_contract(*res)

    ipa = ValidBrokerPosAnalyse(date)
    r = ipa.brokers_sum_up("broker_position_information_score")
    ipa.save_summed_broker_score_data(r, "broker_position_information_score")
    r = ipa.brokers_sum_up("broker_pos_chg_information_score")
    ipa.save_summed_broker_score_data(r, "broker_pos_chg_information_score")
    for r in ipa.main_contract_valid_position_analyse():
        ipa.save_main_contract_valid_position_analyse_result(r)
    for r in ipa.main_contract_valid_pos_chg_analyse():
        ipa.save_main_contract_valid_pos_chg_analyse_result(r)
    for r in ipa.symbol_valid_position_analyse():
        ipa.save_symbol_valid_position_analyse_result(r)
    for r in ipa.symbol_valid_pos_chg_analyse():
        ipa.save_symbol_valid_pos_chg_analyse_result(r)

    ibpc = IBPosComp(date)
    ibpc.total_up_brokers_position()


def output_models(date: datetime = datetime.now()):
    # CURRENT TRADING DAY MODEL OUTPUT
    future_informed_pos_visual = IBConsistency(date)
    future_informed_pos_visual.plot_and_save()

    tib = TotIBPosStructure(date)
    df = tib.total_up()
    tib.plot_and_save(df)
    tib.sort_out_distinguished_broker_position_details()

    ot = OiTargets(date)
    ot.run_and_buff_oi_targets()

    model_ivp.plot_and_save_iv_position_data()
    model_pricep.plot_and_save_price_position_data()


@try_catch(suppress_traceback=False, catch_args=True, enable_alert=True)
def task_wrapper():
    sub_models()
    output_models()
    print(f"{__file__} --- TASK COMPLETE @{datetime.now()}.")


if __name__ == "__main__":
    dt1 = datetime.now().replace(hour=20, minute=30, second=0, microsecond=0)
    dt2 = (datetime.now() + timedelta(days=1)).replace(hour=20, minute=30, second=0, microsecond=0)
    dt = dt1 if datetime.now() < dt1 else dt2
    k = input(
        f"{__file__} SCHEDULE MODEL UPDATE ON {datetime.now().strftime('%Y-%m-%d')} @ {dt}"
        f"\n1. schedule task.\n2. run task\n3. run historical model updates\n"
    )
    if k == '1':
        scheduler = APSchedulerBase()
        scheduler.register_task(task_wrapper, trigger='date', run_date=dt)
        scheduler.start()
    elif k == "2":
        task_wrapper()
    elif k == "3":
        end_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt + timedelta(days=-120)
        confirm_inp = input(
            f"please enter YES to confirm that you need to run sub model from {start_dt} to {end_dt}, "
            f"and run output model on {end_dt}\n"
        )
        if confirm_inp == 'YES':
            for t in yield_dates(start_dt, end_dt):
                sub_models(t)
                print(f"sub model on {t} complete.")
            output_models(end_dt)
        print(f'task complete with output on {end_dt}.')

