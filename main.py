from csv_reader import main

if __name__ == '__main__':
    main(
        customers_path='config/customers.csv',
        items_path='config/items.csv',
        from_csvs='./csvs/',
        to_csvs='./handled_csvs/',
        creds_path='./config/credentials.json',
        spreadsheet_id='1I-pZ071d2fb7kR7gkwMBgY0-rk7RbEPisFL9ZoDTKnM',
        gid=2051596882
    )
