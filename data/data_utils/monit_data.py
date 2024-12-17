import pandas as pd
from utils.database.unified_db_control import UnifiedControl


class MonitData:
    def __init__(self, db_type: str):
        self.udc = UnifiedControl(db_type=db_type)

    def base_check_single_latest_record_datetime(
            self,
            db: str,
            tb: str,
            by: str = 'datetime',
    ):
        last_ = self.udc.read_dataframe(
            db, tb,
            ascending=[(by, False)],
            filter_columns=[by],
            filter_row_limit=1
        )
        last_.columns = ['last_datetime']
        last_['db'] = db
        last_['tb'] = tb
        return last_

    def check_single_latest_record_datetime(
            self,
            db: str,
            tb: str,
    ):
        try:
            res_ = self.base_check_single_latest_record_datetime(db, tb)
        except:
            try:
                res_ = self.base_check_single_latest_record_datetime(db, tb, 'trading_date')
            except:
                try:
                    res_ = self.base_check_single_latest_record_datetime(db, tb, 'listed_date')
                except:
                    res_ = pd.DataFrame()
                else:
                    pass
            else:
                pass
        else:
            pass
        return res_

    def check_all_latest_record_datetime(self):
        res_df = pd.DataFrame()
        # by default, only processed level is monitored and md data is excluded.
        for db in [i for i in self.udc.get_db_names() if "data" in i and "applied" in i]:
            for tb in self.udc.get_table_names(db):
                res_ = self.check_single_latest_record_datetime(db, tb)
                res_df = res_df.append(res_)
        res_df = res_df[['db', 'tb', 'last_datetime']]
        res_df.sort_values(by='last_datetime', ascending=True, inplace=True)
        return res_df


if __name__ == "__main__":
    Mo = MonitData("base")
    df = Mo.check_all_latest_record_datetime()
    from utils.tool.mailing import send_mail
    send_mail("datamonit", df.to_html(), content_type='html', receiver=['233881336@qq.com'])
