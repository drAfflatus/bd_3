import psycopg2

class DBClient:
    """Класс, обеспечивающий доступ к клиентам базы данных"""
    def __init__(self, db, user, password):
        self.db = db
        self.user = user
        self.password = password

    def connect_db(self):
        """метод для соединения с БД """
        self.conn = psycopg2.connect(
            database=self.db,
            user=self.user,
            password=self.password)
        self.cur = self.conn.cursor()

    def disconnect_db(self):
        """метод для отключения соединения с БД """
        self.cur.close()
        self.conn.close()

    def create_tables(self):
        """метод для создания таблиц в БД """
        self.cur.execute("""
                CREATE TABLE IF NOT EXISTS client_db(
                name VARCHAR(30) NOT NULL,
                surname VARCHAR(30) NOT NULL,
                email VARCHAR(40) NOT NULL unique,
                id_cl SERIAL PRIMARY KEY
                );
                """)
        self.cur.execute("""
                CREATE TABLE IF NOT EXISTS phones(
                phone_number VARCHAR(30) NOT NULL,
                id_cl integer NOT NULL references client_db(id_cl)
                );
                """)

        self.conn.commit()

    def add_client(self, name, surname, email, phone_num=""):
        """метод для добавления клиента  с БД """
        if name  and surname and email:

            try:
                self.cur.execute("""
                INSERT INTO client_db
                (name,surname,email)
                VALUES(%s,%s,%s) 
                RETURNING id_cl;
                """, (name, surname, email))
            except psycopg2.Error as ex:
                #print(int(ex.pgcode),type)
                if int(ex.pgcode) == 23505:
                    print('Ошибка уникальности . Пользователь с таким email уже существует')
                self.conn.rollback()
                return
            this_client = self.cur.fetchone()
            if phone_num != "":
                self.cur.execute("""
                            INSERT INTO phones
                            (phone_number,id_cl)
                            VALUES (%s,%s);
                            """, (phone_num, this_client))
            self.conn.commit()
        else:
            self.conn.rollback()
            print("Не заполнены обязательные поля. Данные не сохранены. ")

    def append_phone(self, email="", app_phone=""):
        """метод для добавления телефона  клиенту """
        if email != "" and app_phone != "":
            self.cur.execute("""
                                    SELECT id_cl 
                                    FROM client_db
                                    WHERE
                                    email = %s;
                                    """, (email,))
            id_client = self.cur.fetchone()
            self.cur.execute("""
                        INSERT INTO phones
                        (phone_number,id_cl)
                        VALUES (%s,%s);
                        """, (app_phone, id_client))
            self.conn.commit()

    def remove_phone(self, email="", number_phone=""):
        """метод для удаления телефона клиента """
        self.cur.execute("""
                                   DELETE FROM phones p
                                   USING client_db c 
                                   WHERE  
                                   p.id_cl = c.id_cl and c.email=%s and p.phone_number = %s;
                                   """, (email,number_phone))
        self.conn.commit()

    def change_client(self, email="", new_name="", new_surname="", new_email="", new_phone=""):
        """метод для изменения данных клиента """
        if email != "":
            self.cur.execute("""
                                    SELECT id_cl,name,surname,email
                                    FROM client_db
                                    WHERE
                                    email = %s;
                                    """, (email,))
            client_tuple = self.cur.fetchone()
            id_client = client_tuple[0]
            if not(new_name):
                new_name = client_tuple[1]
            if not(new_surname):
                new_surname = client_tuple[2]
            if not(new_email):
                new_email = client_tuple[3]
            try:
                self.cur.execute("""
                UPDATE client_db
                SET name=%s,surname=%s,email=%s
                WHERE id_cl = %s;
                """, (new_name, new_surname, new_email, id_client))
            except psycopg2.Error as ex:
                #print(int(ex.pgcode),type)
                if int(ex.pgcode) == 23505:
                    print('Ошибка уникальности . Пользователь с таким email уже существует')
                self.conn.rollback()
                return

            if new_phone:
                self.cur.execute("""
                    INSERT INTO phones
                    (phone_number,id_cl)
                    VALUES (%s,%s);
                    """, (new_phone, id_client))
            self.conn.commit()

    def remove_client(self, email=""):
        """метод для удаления  клиента """
        self.cur.execute("""
                           DELETE FROM phones p
                           USING client_db c 
                           WHERE  
                           p.id_cl = c.id_cl and c.email=%s;
                           DELETE from client_db 
                           WHERE 
                           email=%s;
                           """, (email, email))
        self.conn.commit()

    def drop_tables(self):
        """метод для удаления таблиц """

        self.cur.execute("""
                           DROP TABLE  IF EXISTS phones;
                           DROP TABLE  IF EXISTS client_db;
                           """)


    def search_filter(self, name, surname, email, phone):
        """метод для поиска клиента по телефону или по одному/нескольким полям"""
        if phone:
            sql_text = f"""SELECT *
                FROM client_db 
                WHERE 
                id_cl = (
                select id_cl 
                from phones 
                where trim(phone_number) = trim(%s)); 
                """
            print(sql_text)
            self.cur.execute(sql_text, (phone,))
            data_client = self.cur.fetchall()
            return data_client

        s = []
        if name:
            s.append(f"name = '{name}'")
        if surname:
            s.append(f"surname = '{surname}'")
        if email:
            s.append(f"email = '{email}'")
        if len(s)==0:
            return None
        ss = " and ".join(s) + ";"
        sql_text = f"""SELECT * 
            FROM client_db 
            WHERE 
            {ss}"""
        print(sql_text)
        self.cur.execute(sql_text)
        data_client = self.cur.fetchall()
        return data_client

if __name__ == '__main__':
    client = DBClient('nnn', 'postgres', '355037')
    client.connect_db()

    client.drop_tables()
    input("1 Создание Таблиц <<press enter>>")
    client.create_tables()

    # Уникальность клиента обусловлена емайлом
    # двух пользователей с одинаковыми майлами быть не может.
    input("2 Добавление новых клиентов  <<press enter>>")
    client.add_client("John", "Doe", "soap@mail.com", "888 7777")
    client.add_client("Freddy", "Krupp", "soapmail@gmail.com")
    client.add_client("111", "111", "1@1", "1111111111")
    client.add_client("Kurt", "Brumergeld", "ghhf@rambler.ru", "8897786767867688")
    client.add_client("Freddy", "Morgan", "fred56@ya.com", "11111-4444")

    input("3 Добавление телефона существующему клиенту  <<press enter>>")
    client.append_phone("soapmail@gmail.com", "55 66 77")

    input("4 Изменение данных клиента <<press enter>>")
    client.change_client("soap@mail.com", "Ivan", "", "shamppoo@mail.com", "999 66666")

    input("5 Удаление телефона существующему клиенту <<press enter>>")
    client.remove_phone('ghhf@rambler.ru', "8897786767867688")

    input("6 Удаление существующего клиента <<press enter>>")
    client.remove_client('ghhf@rambler.ru')

    input("7 Поиск по: имени и/или фамилии и/или почте   или телефону  <<press enter>>")
    print("-" * 80)
    res = client.search_filter("Freddy", "", "", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("Freddy", "111", "", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("", "", "shamppoo@mail.com", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("", "", "", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("Freddy", "Krupp", "", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("Freddy", "", "soapmail@gmail.com", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("", "Krupp", "soapmail@gmail.com", "")
    print(res)
    print("-" * 80)
    res = client.search_filter("", "", "", "999 66666")
    print(res)
    client.disconnect_db()
