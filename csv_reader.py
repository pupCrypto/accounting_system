import os
import sys
import time
import shutil
import logging
import datetime
from pathlib import Path
from actions import ActionBuilder
from spreadsheet import AccountingSpreadsheet

import telebot

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('./logs/logs.log')
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)
logger.addHandler(file_handler)


def read_file(file_path: str | Path) -> list[str]:
    """
    Return list of str read csv file
    """
    with open(file_path, 'r', encoding='utf8') as csv_file:
        return [line.rstrip('\n') for line in csv_file.readlines()]


def main(
        customers_path: str,
        items_path: str,
        from_csvs: str,
        to_csvs: str,
        creds_path: str,
        spreadsheet_id: str,
        gid: int,
        telegram_bot_token: str | None = None,
        chat_id: int | None = None,
):  # noqa
    """
    Start func
    """
    if not os.path.exists(customers_path):
        raise FileExistsError(f'No such file {customers_path}')
    if not os.path.exists(items_path):
        raise FileExistsError(f'No such file {items_path}')
    if not os.path.exists(from_csvs):
        raise FileExistsError(f'No such folder {from_csvs}')
    if not os.path.exists(to_csvs):
        raise FileExistsError(f'No such folder {to_csvs}')
    if not os.path.exists(creds_path):
        raise FileExistsError(f'No such file {creds_path}')

    os.environ['CUSTOMER_PATH'] = customers_path
    os.environ['ITEMS_PATH'] = items_path
    print('Started!')
    while True:
        files = os.listdir(from_csvs)
        files_with_date = [
            {'date': datetime.datetime.strptime(f.split('_').pop(1), '%Y-%m-%d').date(), 'file_name': f}
            for f in files
        ]
        files_with_date.sort(key=lambda file_with_date: file_with_date['date'])
        g = AccountingSpreadsheet(spreadsheet_id, gid, creds_path, logger)
        for f in files_with_date:
            file_name = f['file_name']

            source = Path(from_csvs) / file_name
            dest = Path(to_csvs) / file_name

            logger.info(f'Trying to read file along path {source}')
            codes = read_file(source)
            logger.info(f'Read data: {codes}')

            builder = ActionBuilder.get_builder(codes)
            operation, params = builder.build()
            logger.info(f'Trying to handle operation: {operation.upper()}. Params: {params}')
            try:
                if operation == 'sale':
                    g.create_sale(params, f['date'])
                elif operation == 'income':
                    g.create_income(params, f['date'])
                elif operation == 'shipment':
                    g.create_shipment(params, f['date'])
                elif operation == 'inventory':
                    g.do_inventory(params, f['date'] - datetime.timedelta(days=1))
                shutil.move(source, dest)
                logger.info(f'File {source} was moved to {dest}')
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error(f'Error was occurred. Error type: {type(e)}.\n'
                             f'Error content: {str(e)}\n'
                             f'Error line: {exc_tb.tb_lineno}\n'
                             f'Error file: {fname}')

                # save problem csv
                if not os.path.exists('./error_csvs'):
                    os.mkdir('./error_csvs')
                error_csv_path = Path('./error_csvs') / file_name
                if not os.path.exists(error_csv_path):
                    shutil.copyfile(source, error_csv_path)

                # send message about problem csv
                if telegram_bot_token is not None and chat_id is not None:
                    try:
                        bot = telebot.TeleBot(telegram_bot_token)
                        bot.send_message(
                            chat_id,
                            f'При обработке файла **{file_name}** произошла ошибка ❌'
                        )
                    except Exception as e:
                        logger.error(f'Error was with telegram bot.\n'
                                     f'Error type: {type(e)}\n'
                                     f'Error content: {str(e)}')
        del g
        if len(files_with_date) > 0:
            logger.info('All files were handled')
        else:
            logger.info(f'No files were found in "{from_csvs}" folder')
        logger.info('Sleep for 1 hours')
        time.sleep(3600)
