#!/usr/bin/python

import jaydebeapi
conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:demipt/gandalfthegrey@de-oracle.chronosavant.ru:1521/deoracle',['demipt','gandalfthegrey'],'/home/demipt/ojdbc8.jar')
curs = conn.cursor()

# Функция очистки STG
def declutter_stg(table_name):
    sql = 'delete from '+ table_name
    curs.execute(sql)

# Функция для захвата данных в STG

def insert_stg(stg_tab_name, stg_attr_list, src_attr_list, src_tab_name, meta_tab_name, db_name, dwh_tab_name):
    sql = """insert into {0} ({1}, create_dt, update_dt  ) select {2} from  {3}
            where coalesce(update_dt, create_dt) > (select last_update from {4}
            where dbname = '{5}' and tablename = '{6}')
        """.format(stg_tab_name, stg_attr_list, src_attr_list, src_tab_name, meta_tab_name, db_name, dwh_tab_name)
    curs.execute(sql)

# Функция для переливания измерений из STG в хранилище SCD2

def insert_scd2(dwh_tab_name, dwh_attr_list, stg_attr_list, stg_tab_name):
    sql = """insert into {0} ({1}) select {2}, coalesce( update_dt, create_dt ), to_date( '2999-12-31', 'YYYY-MM-DD' ), 'N' from {3}
        """.format(dwh_tab_name, dwh_attr_list, stg_attr_list, stg_tab_name)
    curs.execute(sql)

# Функция для обновления данных в хранилище SCD2

def merge_scd2(dwh_tab_name, stg_tab_name, dwh_key, scd_key): 
    sql = """merge into {0} tgt using {1} src on (tgt.{2} = src.{3} and
            tgt.effective_from < src.update_dt) 
            when matched then update set 
            tgt.effective_to = src.update_dt - interval '1' second 
            where tgt.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' )
        """.format(dwh_tab_name, stg_tab_name, dwh_key, scd_key)
    curs.execute(sql)

# Функция для обновления метаданных (даты последней загрузки в хранилище)

def upd_meta(meta_tab_name, stg_tab_name, db_name, short_dwh_tab_name):
    sql = """update {0} set last_update = ( select max( coalesce( update_dt, create_dt ) ) from {1})
            where  dbname = '{2}' and tablename = '{3}' and (select max( coalesce( update_dt, create_dt ) ) from {1}) is not null
        """.format(meta_tab_name, stg_tab_name, db_name, short_dwh_tab_name)
    curs.execute(sql)




