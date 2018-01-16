import logging
import psycopg2
import datetime
import threading
import random

class InputZp(threading.Thread):

    conn        = None # psycopg2.extensions.connection
    conn_string = None
    users_dict  = None
    log_dict    = None
    name               = None
    bv_input_period_id = 0
    bv_user_id = 0 # 18408803
    bv_user_login = None
    l_frms_status_1 = 0
    is_running = 0
    oiv_list = []
    p_oiv_list = str("")
    terr_list = []
    p_terr_list = str("")
    p_form_list = []

    def __init__(self, threadID,conn_string:str, users_dict:list, log_dict:dict, app_db_connect_name:str):
        '''Class constructor, set some initial variables.
        conn_string : connect string, "host='xxx.xxx.xxx.xxx' dbname='db' user='prm' password='prm'"
        If conn variable is not None then is used, open, actions, close - else using connect string to open
        new connection: open, actions, close.
        users_dict - common object (list of tuples), contains all users (locked by InputZp instance, one user has one db session)
        log_dict   - common object (dict), contains log information about user actions, like :
                     sys datet time, user_id, connect open time, calls times.
        Dict is mutable object, all instances of InputZp use single memory instance of dict: users_dict, log_dict
        app_db_connect_name - application_name if not None then set after connection
        '''
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conn_string = conn_string
        self.users_dict = users_dict
        self.log_dict = log_dict
        self.name = app_db_connect_name

        random_rn = random.randint(0, len(self.users_dict)-1)
        self.bv_user_id = int(self.users_dict[random_rn][1])
        self.bv_user_login = self.users_dict[random_rn][2]

    def get_threadID(self):
        return self.threadID

    def get_name(self):
        return self.name

    def get_is_running(self):
        return self.is_running

    def open_db_connection(self):
        '''
        Open connection to db by self.conn_string
        return: float duration in seconds.
        '''
        t_begin = datetime.datetime.now()
        self.conn = psycopg2.connect(self.conn_string)
        if self.name is not None:
            cur = self.conn.cursor()
            cur.execute("set application_name TO '" + self.name + "'; ")
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        return delta.total_seconds()

    def get_period_list(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_input_period_list(refcur => 'qwe');")
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            self.bv_input_period_id = int(row[0])
        cur_res.close()
        logging.info("Use last row from periods :  bv_input_period_id=" + str(self.bv_input_period_id))
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        return delta.total_seconds()

    def get_form_status_combo(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_status_list(refcur        => 'qwe', "
                                                                                "p_input_period_id => %(p_input_period_id)s, "
                                                                                "p_user_id         => %(p_user_id)s);",
                                                                                {'p_input_period_id': self.bv_input_period_id,
                                                                                 'p_user_id':self.bv_user_id})
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            if (row[0] == 1 and row[4] is not None):
                self.l_frms_status_1 = int(row[4])
        cur_res.close()
        if self.l_frms_status_1 > 0:
            logging.info("self.l_frms_status_1 : " + str(self.l_frms_status_1))
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        return delta.total_seconds()

    def get_period_close_date_combo(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_get_period_close_date(refcur => 'qwe');")
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
        cur_res.close()
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        return delta.total_seconds()

    def get_okved_combo(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_okved_list(refcur => 'qwe', "
                                                             "p_input_period_id => %(p_input_period_id)s);",
                                                            {'p_input_period_id': self.bv_input_period_id})
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            #print(str(row, ))
        cur_res.close()
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        return delta.total_seconds()

    def get_oiv_combo(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_oiv_list(refcur            => 'qwe', "
                                                                                 "p_input_period_id => %(p_input_period_id)s, "
                                                                                 "p_user_id         => %(p_user_id)s);",
                                                                         {'p_input_period_id': self.bv_input_period_id,
                                                                                  'p_user_id': self.bv_user_id})
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            #print(row[0])
            #print(str(row, ))
            self.oiv_list.append(row[0])
        cur_res.close()
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()

        # prepare string
        #'{\"2\",\"69394\"}'
        self.p_oiv_list = '{'
        self.p_oiv_list += ','.join(str(x) for x in self.oiv_list)
        self.p_oiv_list += '}'

        #for x in self.oiv_list:
        #    self.p_oiv_list += '\"'+str(x)+'\"'
        #    if x != self.oiv_list[]
        print("OIV:" + ','.join(str(x) for x in self.oiv_list))
        #print(self.p_oiv_list)
        return delta.total_seconds()

    def get_terr_combo(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("BEGIN; SELECT * FROM prm_salary.pkg_web_salary_form_territory_list(refcur            => 'qwe', "
                                                                                       "p_input_period_id => %(p_input_period_id)s );",
                                                                                      {'p_input_period_id': self.bv_input_period_id})
        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            # print(str(row, ))
            self.terr_list.append(row[0])
        cur_res.close()
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        self.p_terr_list = '{'
        self.p_terr_list += ','.join(str(x) for x in self.terr_list)
        self.p_terr_list += '}'
        print("TERR:" + ','.join(str(x) for x in self.terr_list))
        return delta.total_seconds()

    def get_form_report_list(self):
        t_begin = datetime.datetime.now()
        cur = self.conn.cursor()
        cur.execute("begin; select * from prm_salary.pkg_web_salary_form_report_list(refcur => 'qwe',"
                    "p_input_period_id     => %(p_input_period_id)s,"
                    "p_user_id             => %(p_user_id)s,"
                    "P_FORM_TYPE_LIST      => '{\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"-1\"}',"
                    "P_OIV_LIST            => %(p_oiv_list)s,"
                    "P_OKVED_LIST          => '{\"35\",\"36\",\"37\",\"38\",\"41\",\"42\",\"43\",\"45\",\"49\",\"50\",\"52\",\"53\",\"55\",\"56\",\"58\",\"59\",\"60\",\"62\",\"63\",\"64\",\"66\",\"68\",\"69\",\"70\",\"71\",\"72\",\"73\",\"74\",\"75\",\"78\",\"80\",\"81\",\"82\",\"84\",\"85\",\"86\",\"87\",\"88\",\"90\",\"91\",\"93\",\"96\",\"01\",\"01.61\",\"02\",\"02.1\",\"02.10\",\"02.40\",\"02.40.1\",\"35.12\",\"35.30\",\"35.30.14\",\"36.0\",\"36.00\",\"36.00.2\",\"37.00\",\"38.1\",\"41.2\",\"41.20\",\"42.21\",\"43.99\",\"43.99.9\",\"45.20\",\"49.3\",\"49.31\",\"49.31.2\",\"49.31.22\",\"49.41\",\"49.41.2\",\"50.30\",\"52.10\",\"52.10.4\",\"52.21\",\"52.21.2\",\"52.21.22\",\"52.21.24\",\"53.10\",\"53.10.4\",\"53.10.9\",\"55.20\",\"55.90\",\"56.29\",\"58.13\",\"59.1\",\"59.13\",\"59.14\",\"60.10\",\"62.02\",\"63.11\",\"63.11.1\",\"63.91\",\"64.99\",\"64.99.3\",\"66.19\",\"66.19.6\",\"68.10\",\"68.10.12\",\"68.20\",\"68.20.2\",\"68.3\",\"68.32\",\"68.32.1\",\"68.32.2\",\"69.10\",\"69.20\",\"69.20.2\",\"70.22\",\"71.1\",\"71.11\",\"71.11.1\",\"71.12\",\"71.12.1\",\"71.12.2\",\"71.12.4\",\"71.12.41\",\"71.12.46\",\"71.12.53\",\"72.19\",\"72.20\",\"73.20\",\"73.20.1\",\"74.20\",\"75.00\",\"75.00.1\",\"75.00.2\",\"78.10\",\"78.20\",\"80.10\",\"80.3\",\"80.30\",\"81.10\",\"81.29\",\"81.29.2\",\"81.29.9\",\"82.11\",\"82.19\",\"82.99\",\"84.1\",\"84.11\",\"84.11.22\",\"84.11.3\",\"84.11.31\",\"84.11.35\",\"84.11.4\",\"84.11.8\",\"84.12\",\"84.13\",\"84.22\",\"84.25\",\"84.25.1\",\"84.25.9\",\"85.11\",\"85.12\",\"85.13\",\"85.14\",\"85.21\",\"85.22\",\"85.23\",\"85.41\",\"85.41.1\",\"85.41.2\",\"85.41.9\",\"85.42\",\"85.42.9\",\"86.1\",\"86.10\",\"86.21\",\"86.22\",\"86.23\",\"86.90\",\"86.90.1\",\"86.90.2\",\"86.90.4\",\"86.90.9\",\"87.3\",\"87.30\",\"87.9\",\"87.90\",\"88.1\",\"88.10\",\"88.91\",\"88.99\",\"90.0\",\"90.01\",\"90.03\",\"90.04\",\"90.04.2\",\"90.04.3\",\"91.0\",\"91.01\",\"91.02\",\"91.03\",\"91.04\",\"93.1\",\"93.11\",\"93.12\",\"93.19\",\"93.2\",\"93.21\",\"93.29\",\"93.29.2\",\"93.29.9\",\"96.0\",\"96.03\",\"96.04\"}',"
                    "P_REORGANIZATION_LIST => '{\"1\",\"2\"}',"
                    "P_TERRITORY_LIST      => %(p_terr_list)s,"
                    "P_STATUS_LIST         => '{\"1\",\"4\",\"5\",\"3\",\"9\",\"6\",\"7\",\"8\"}',"
                    "P_USE_FILTER          => '{\"1\",\"5\",\"2\",\"3\",\"4\"}',"
                    "p_string_filter       => '',"
                    "p_first_row           => 1,"
                    "p_last_row            => 9999999999);",
                             {'p_input_period_id': self.bv_input_period_id,
                              'p_user_id': self.bv_user_id,
                              'p_oiv_list': self.p_oiv_list,
                              'p_terr_list': self.p_terr_list
                             }
                    )

        cur_res = self.conn.cursor('qwe')
        row_count = 0
        for row in cur_res:
            row_count += 1
            logging.debug(str(row_count) + " " + str(row, ))
            #print("FORM= ",str(row, ))
            #print("FORM= ", str(row[0]))
            self.p_form_list.append(row[0])
        cur_res.close()
        print(">>> USER HAS ",str(row_count)," FORMS >>>")
        t_end = datetime.datetime.now()
        delta = t_end - t_begin
        cur.close()
        print("FORMS CNT(",str(row_count),"):"+','.join(str(x) for x in self.p_form_list))
        return delta.total_seconds()


    def run(self):
        '''
         Run script(s) of test.
        '''
        if self.is_running == 1:
            return

        self.is_running = 1
        open_conn_sec = self.open_db_connection()
        #print("[",self.name,"] open_conn_sec = ",str(open_conn_sec))

        get_periods_time = self.get_period_list()
        frm_status_combo_time = self.get_form_status_combo()
        close_date_combo = self.get_period_close_date_combo()
        okved_combo_time = self.get_okved_combo()
        oiv_combo = self.get_oiv_combo()
        terr_combo = self.get_terr_combo()
        form_list = self.get_form_report_list()

        # print("--- USER_ID --- [",self.bv_user_id,"]")

        print("[", self.name, "] user_id = ",self.bv_user_id," login=",self.bv_user_login," openc = ", str(open_conn_sec),
               " per_list = ",str(get_periods_time),
               " frm_stat = ",str(frm_status_combo_time),
               " close_dat = ",str(close_date_combo),
               " okved_cmb = ",str(okved_combo_time),
               " oiv_cmb = ",str(oiv_combo),
               " terr_cmb = ",str(terr_combo),
               " frm_lst = ",str(form_list)
              )

        #delit
        cur = self.conn.cursor()
        self.conn.close()
        self.is_running = 0
        #cur.execute("select pg_sleep(10)")
        #self.name.exit()
