from MainInputZp import InputZp
import psycopg2
from time import sleep
import random

def main():
    users_dict = {}
    log_dict   = {}
    conn_string = "host='10.242.5.62' dbname='db_ris_mkrpk' user='prm_salary' password='prm_salary'" #DERS
    #conn_string = "host='46.146.228.115' dbname='db_ris_mkrpk' user='prm_salary' password='prm_salary'" #MTS


    conn_ext = psycopg2.connect(conn_string)
    cur = conn_ext.cursor()
    cur.execute("select row_number() over() as rn,user_id,user_login from prm_admin.t_users u where u.is_enabled=1 and u.user_login in ('yakushevan','volokitinanv','myrzinaa')")
    # records - list of tuples
    records = cur.fetchall()
    #random_rn = random.randint(1,len(records))
    #print(int(records[random_rn][1]))
    cur.close()
    conn_ext.close()

    #=======================
    #return

    list_of_InpZp = []
    n = 1

    for i in range(1,n+1):
        list_of_InpZp.append(InputZp(i, conn_string,records,log_dict,'inst_'+str(i)))
        inst=list_of_InpZp[-1] # get last element of list
        inst.start()

    # последовательный запуск без ожидания.
    # см.
    '''
    sleep(5)
    for j in range(100):
        random.seed()
        sleep(random.uniform(0.1, 0.5))
        rv_inst_num = random.randint(1, n-2)
        print("RANDOM rv_inst_num=",rv_inst_num)
        inst = list_of_InpZp[rv_inst_num]
        if inst.get_is_running() == 0:
            thID = inst.get_threadID()
            instname = inst.get_name()
            del inst
            print("Create new instance with name = ",instname)
            inst = InputZp(thID, conn_string, records, log_dict, instname)
            inst.start()
        else:
            print("inst = ",inst.get_name()," is running")
    '''

    '''
    for i in range(1,n):
     inst = list_of_InpZp[i-1]
     sleep(random.uniform(0.3, 1.5))
     if inst.get_is_running()==0:
         inst.start()
    '''

main()