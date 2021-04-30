#!/usr/bin/python

# Загружаем библиотеки
import pandas
import os
import jaydebeapi
from py_scripts.my_module import declutter_stg, insert_stg, insert_scd2, merge_scd2, upd_meta

# Установка соединения с сервером
conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:demipt/gandalfthegrey@de-oracle.chronosavant.ru:1521/deoracle',['demipt','gandalfthegrey'],'/home/demipt/ojdbc8.jar')
conn.jconn.setAutoCommit(False)
curs = conn.cursor()

# Объявляем переменные
meta_tab = 'DEMIPT.SNZH_META_LOAD'

src_attr_clients = 'client_id, last_name, first_name, patronymic, date_of_birth,\
                    passport_num, passport_valid_to, phone, create_dt, update_dt'

stg_attr_clients = 'client_id, last_name, first_name, patronymic, date_of_birth,\
                    passport_num, passport_valid_to, phone'

dwh_attr_clients = 'client_id, last_name, first_name, patronymic,date_of_birth,\
                    passport_num, passport_valid_to, phone,\
	            effective_from, effective_to, deleted_flg'

stg_key_clients = 'client_id'
dwh_key_clients = 'client_id'

src_attr_accounts = 'account, valid_to, client, create_dt, update_dt'
stg_attr_accounts = 'account_num, valid_to, client'
dwh_attr_accounts = 'account_num, valid_to, client,\
                     effective_from, effective_to, deleted_flg'
stg_key_accounts = 'account_num'
dwh_key_accounts = 'account_num'

src_attr_cards = 'card_num, account, create_dt, update_dt'
stg_attr_cards = 'card_num, account_num'
dwh_attr_cards = 'card_num, account_num, effective_from, effective_to, deleted_flg'
stg_key_cards = 'card_num'
dwh_key_cards = 'card_num'

stg_tab_list = ['DEMIPT.SNZH_STG_CLIENTS', 'DEMIPT.SNZH_STG_ACCOUNTS', 'DEMIPT.SNZH_STG_CARDS',
            'DEMIPT.SNZH_STG_TRANSACTIONS', 'DEMIPT.SNZH_STG_TERMINALS', 
            'DEMIPT.SNZH_STG_PSSPRT_BLACKLIST', 'DEMIPT.SNZH_STG_CLIENTS_KEYS',
            'DEMIPT.SNZH_STG_ACCOUNTS_KEYS', 'DEMIPT.SNZH_STG_CARDS_KEYS', 
            'DEMIPT.SNZH_STG_TERMINALS_KEYS']

# Очистка STG
for table_name in stg_tab_list:
    declutter_stg(table_name)


# Загрузка файлов из сторонних источников
path = '/home/demipt/SNZH/'
field = os.listdir(path)

## Загрузка паспортов
for file in field:
    index = file.find('pas')
    if index != -1:
        passports = file[index:]
passp_bl_list_df = pandas.read_excel(passports, sheet_name='blacklist', header=0, index_col=None) 
passp_bl_list_df['date'] = passp_bl_list_df['date'].astype(str)

## Загрузка терминалов
for file in field:
    index = file.find('ter')
    if index != -1:
        terminals = file[index:]
date_terminals = terminals.split('_')[1].split('.')[0]
terminals_df = pandas.read_excel(terminals, sheet_name='terminals', header=0, index_col=None)

## Загрузка транзакций
for file in field:
    index = file.find('tr')
    if index != -1:
        transactions = file[index:]
transactions_df = pandas.read_csv(transactions, sep = ';', header=0, index_col=None)
transactions_df['transaction_id'] = transactions_df['transaction_id'].astype(str)
rep_date = transactions.split('_')[1].split('.')[0]

# Перемещение файлов в архив
passp_path_old = path + passports
passp_path_new = path +'archive/'+ passports + '.backup'
term_path_old = path + terminals
term_path_new = path + 'archive/' +terminals + '.backup'
trans_path_old = path + transactions
trans_path_new = path + 'archive/' + transactions + '.backup'

os.rename(passp_path_old, passp_path_new)
os.rename(term_path_old, term_path_new)
os.rename(trans_path_old, trans_path_new)

# Выполнение ETL для сведений, хранящихся в СУБД ORACLE
## Захват данных в STG
insert_stg(stg_tab_name= 'DEMIPT.SNZH_STG_CLIENTS', 
        stg_attr_list = stg_attr_clients, 
        src_attr_list = src_attr_clients,
        src_tab_name = 'BANK.CLIENTS', 
        meta_tab_name = meta_tab, db_name = 'DEMIPT', 
        dwh_tab_name = 'SNZH_DWH_DIM_CLIENTS_HIST')

insert_stg( stg_tab_name = 'DEMIPT.SNZH_STG_ACCOUNTS', 
        stg_attr_list = stg_attr_accounts, 
        src_attr_list = src_attr_accounts,
        src_tab_name = 'BANK.ACCOUNTS', 
        meta_tab_name = meta_tab, db_name = 'DEMIPT', 
        dwh_tab_name = 'SNZH_DWH_DIM_ACCOUNTS_HIST')
    
insert_stg(stg_tab_name= 'DEMIPT.SNZH_STG_CARDS', 
        stg_attr_list = stg_attr_cards, 
        src_attr_list = src_attr_cards,
        src_tab_name = 'BANK.CARDS', 
        meta_tab_name = meta_tab, db_name= 'DEMIPT', 
        dwh_tab_name = 'SNZH_DWH_DIM_CARDS_HIST')

## Переливаем измерения из STG в хранилище SCD2
insert_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_CLIENTS_HIST', 
dwh_attr_list = dwh_attr_clients, 
stg_attr_list = stg_attr_clients,
stg_tab_name = 'DEMIPT.SNZH_STG_CLIENTS')

insert_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_ACCOUNTS_HIST', 
dwh_attr_list = dwh_attr_accounts,
stg_attr_list = stg_attr_accounts,
stg_tab_name = 'DEMIPT.SNZH_STG_ACCOUNTS')

insert_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_CARDS_HIST', 
dwh_attr_list = dwh_attr_cards, 
stg_attr_list = stg_attr_cards,
stg_tab_name = 'DEMIPT.SNZH_STG_CARDS')

## Обновляем данные в хранилище SCD2
merge_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_CLIENTS_HIST', 
    stg_tab_name = 'DEMIPT.SNZH_STG_CLIENTS', 
    dwh_key = dwh_key_clients, 
    scd_key = stg_key_clients)

merge_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_ACCOUNTS_HIST', 
    stg_tab_name = 'DEMIPT.SNZH_STG_ACCOUNTS', 
    dwh_key = dwh_key_accounts, 
    scd_key = stg_key_accounts)

merge_scd2(dwh_tab_name = 'DEMIPT.SNZH_DWH_DIM_CARDS_HIST', 
    stg_tab_name = 'DEMIPT.SNZH_STG_CARDS', 
    dwh_key = dwh_key_cards, 
    scd_key = stg_key_cards)

# Обработка удалений
with open('/home/demipt/SNZH/sql_scripts/del_sql.txt', 'r') as file:
    sql = file.read()
for sql_statement in sql.split(";"):
	curs.execute(sql_statement)

## Обновляем метаданные - дату максимальной загрузки
upd_meta(meta_tab_name = meta_tab, stg_tab_name = 'DEMIPT.SNZH_STG_CLIENTS',
         db_name = 'DEMIPT', short_dwh_tab_name = 'SNZH_DWH_DIM_CLIENTS_HIST')
upd_meta(meta_tab_name = meta_tab, stg_tab_name = 'DEMIPT.SNZH_STG_ACCOUNTS',
         db_name = 'DEMIPT', short_dwh_tab_name = 'SNZH_DWH_DIM_ACCOUNTS_HIST')
upd_meta(meta_tab_name = meta_tab, stg_tab_name = 'DEMIPT.SNZH_STG_CARDS',
         db_name = 'DEMIPT', short_dwh_tab_name = 'SNZH_DWH_DIM_CARDS_HIST')


# Выполнение ETL для сведений, хранящихся в файлах
## Захват данных в STG
with open('/home/demipt/SNZH/sql_scripts/incert_files.txt', 'r') as file:
    sql = file.read().split(";")
    curs.executemany(sql[0], passp_bl_list_df.values.tolist())
    curs.executemany(sql[1], transactions_df.values.tolist())
    curs.executemany(sql[2], terminals_df.values.tolist())

## Реализация остальных этапов ETL
with open('/home/demipt/SNZH/sql_scripts/incr_files.txt', 'r') as file:
    sql = file.read()
for sql_statement in sql.split(";"):
	curs.execute(sql_statement.format(date_terminals))

# Строим отчет
with open('/home/demipt/SNZH/sql_scripts/reports_sql.txt', 'r') as file:
    sql = file.read()
for sql_statement in sql.split(";"):
	curs.execute(sql_statement.format(rep_date))

conn.commit()

# Закрытие соединения
curs.close()
conn.close()
