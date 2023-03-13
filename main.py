from tkinter import *
from tkinter import filedialog
import os
from threading import Thread
import xlsxwriter
import cx_Oracle
from datetime import datetime, date
from tkinter import ttk
from tkcalendar import *
import re
import zlib
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d

# Szerintem hálózaton belül nincs különösebb jelentősége a jelszó elrejtésének, de így nem plaint text-ben van tárolva,
# ettől jobban alszom, és ez a lényeg.


def unobscure(obscured: bytes) -> bytes:
    return zlib.decompress(b64d(obscured))


def db_ugyfel(run, frame, pb):

    # Thread nélkül nem várja meg a python az sql-t, visszatér üresen. Tippem hogy azért, mert ezer évig fut az sql.
    thread_equ = Thread(target=lekerd_equ, args=(run,))
    thread_equ.start()
    pb.start()

    while thread_equ.is_alive():
        for i in range(0, 6):
            pb['value'] = i * 20
            frame.after(1000, frame.update_idletasks())
            frame.after(1, frame.update())
    pb.stop()

    thread_equ.join()

    if equ != []:
        print("Ügyféladat lekérdezés elindul")
        thread_ugyf = Thread(target=lekerd_ugyfel, args=(run,))
        print("Tranzakció lekérdezés elindul")
        thread_tr = Thread(target=lekerdezes, args=(run,))
        thread_ugyf.start()
        thread_tr.start()

        pb.start()
        # Azt hittem ezzel van a baj, közben meg tök jól körbejártam, hogy a GUI miatt crashelt folyton, azért van most
        # egy frame.update is lejjebb
        while thread_ugyf.is_alive():
            for i in range(0, 6):
                pb['value'] = i * 20
                frame.after(1000, frame.update_idletasks())
                frame.after(1, frame.update())
        #     print(thread_ugyf.is_alive())
        # print(thread_ugyf.is_alive())
        pb.stop()

        # thread_ugyf.join()
        # thread_tr.join()
    print("Lekérdezések kész")


def lekerd_equ(run):
    global equ
    equ = []
    # Csatlakozás az adatbázishoz, hibát dob ha rossz a jelszó
    dsn_tns = cx_Oracle.makedsn("prac-scan-bar.khb.hu", 1521, service_name="POLTP_APP.khb.hu")
    try:
        db = cx_Oracle.connect(user=run.user, password=run.passw, dsn=dsn_tns)
    except cx_Oracle.DatabaseError as e:
        raise

    fromd_str = run.fromd.strftime("%Y/%m/%d")
    tod_str = run.tod.strftime("%Y/%m/%d")
    with db.cursor() as cursor:
        # with open(run.file_trans + 'raw_sales_pl_script.sql', 'r') as sql_file:
        with open(run.file_sql + 'raw_sales_pl_script.sql', 'r') as sql_file:
            sql_mod = sql_file.read()
            sql_mod = re.sub('[\s\S]*?(from)', 'select distinct t.CLIENT_EQUATION \n from', sql_mod, 1)
            sql_mod = re.sub('between to_date\([\s\S]*?,', 'between to_date(:fromd,', sql_mod, 1)
            sql_mod = re.sub('and to_date\([\s\S]*?,', 'and to_date(:tod,', sql_mod, 1)
        if not bool(run.reta.get()):
            sql_mod = sql_mod.replace("AND TYPOLOGY LIKE ('%RETA%')", "--AND TYPOLOGY LIKE ('%RETA%')")
        try:
            cursor.execute(sql_mod, {'fromd':fromd_str,'tod':tod_str})
        except cx_Oracle.DatabaseError as e:
            raise

        for r, row in enumerate(cursor.fetchall()):
            sor = str(row)
            # kihagyom az üres equation-ös sorokat, mert teljesen felesleges beleírni a list-be
            if sor != "(None,)":
                # ('######',) formában jönnek elemek, de a zárójelek és idézőjelek nem kellenek egyelőre...
                sor = sor[1:-2]
                equ.append(sor)

        # print(equ)
        # a python list->dictionary->list konvertálással kiszedi az összes ismétlődő elemet
        equ = list(dict.fromkeys(equ))
        # fel kell darabolni
    print("EQU lekérdezés kész")


def lekerd_ugyfel(run):
    # Csatlakozás az adatbázishoz, hibát dob ha rossz a jelszó
    dsn_tns = cx_Oracle.makedsn("pdw-bar.khb.hu", 1521, service_name="pdw.khb.hu")
    try:
        db = cx_Oracle.connect(user=run.user, password=run.passw, dsn=dsn_tns)
    except cx_Oracle.DatabaseError as e:
        raise
    # print(len(equ) // 1000)
    # Az adattárház db-je a query-ket csak ezresével fogadja
    # ezért nekem itt szét kell darabolnom az equation azonosítókat, hogy később az sql-be be lehessen illeszteni ezresével
    # Gyakorlatilag ugyanezt csináltuk eddig lépésről lépésre, csak kézzel

    bind_tb_equ = []

    rows = len(equ) // 1000 + 1
    # Megcsinálja üresen a változót amibe később az azonosítók kerülnek, pont akkorára amekkora kelleni fog
    # (ahányszor 1000 annyi széles, és 1000 mély tömbbe gyömöszköli őket. Például ha 1519 db azonosító van, az 2 széles
    # lesz, egy 1000 hosszú és egy 519 hosszú oszloppal.

    for i in range(rows):
        col = []
        if i == len(equ) // 1000:
            for j in range(len(equ) % 1000):
                col.append("")
        else:
            for j in range(1000):
                col.append("")
        bind_tb_equ.append(col)

    tomb_equ = []
    for i in range(len(equ) // 1000 + 1):
        tomb_equ.append("")

    for i in range(len(equ) // 1000 + 1):
        if i == len(equ) // 1000:
            for j in range(len(equ) % 1000):
                egy_equ = equ[j + i * 1000]
                bind_tb_equ[i][j] = egy_equ[1:-1] #Alapból zárójelek (idézőjelek?) közt vannak az értékek, azt most eldobjuk
                if j != len(equ) % 1000 - 1:
                    tomb_equ[i] = tomb_equ[i] + ", "
        else:
            for j in range(1000):
                tomb_equ[i] = tomb_equ[i] + equ[j + i * 1000]
                egy_equ = equ[j + i * 1000]
                bind_tb_equ[i][j] = egy_equ[1:-1] #Alapból zárójelek (idézőjelek?) közt vannak az értékek, azt most eldobjuk
                if j != 1000 - 1:
                    tomb_equ[i] = tomb_equ[i] + ", "

    run.stamp = timestamp()
    fromd_str = run.fromd.strftime("%Y/%m/%d")
    tod_str = run.tod.strftime("%Y/%m/%d")
    # timestampelt excelt nyit
    # külön kimentem
    wugyf = run.hova + 'prod_ugyfeltabla_' + run.stamp + '.xlsx'
    workbook = xlsxwriter.Workbook(wugyf, {'constant_memory': True, 'default_date_format': 'yyyy/mm/dd'})
    ugyf = workbook.add_worksheet()
    with db.cursor() as cursor:
        with open(run.file_sql + 'Ugyfeltabla.sql', 'r') as sql_file:
            sql_beolvasva = sql_file.read()


        cursor.execute("begin ms_as_sec_audit.set_reason('Scriptelt Sales riport futtatása, felhasználó:" + os.getlogin() + " , futtatás időpontja: " + run.stamp + " ,lekérdezett időszak: " + fromd_str + "-tól " + tod_str + "-ig" + "'); end;")
        db.commit()
        print("Audit beírás rendben")

        for ezres in range(len(equ) // 1000 + 1):
            # Kicseréli az eddig bentlévő sql-hez való, (&parameternev) alakú szöveget (%s)-re, ami az oracle modul által elvárt paraméter típus

            sql_bindolva = re.sub('&[\s\S]*?\)', '%s)', sql_beolvasva, 1)
            # sql_str = kiolvasott_file.replace("<pholder>", tomb_equ[ezres])
            bindolando = [":" + str(i + 1) for i in range(len(bind_tb_equ[ezres]))]
            sql_bindolva = sql_bindolva % (",".join(bindolando))
            # print(len(bind_tb_equ[ezres]))
            try:
                cursor.execute(sql_bindolva, bind_tb_equ[ezres])
            except cx_Oracle.DatabaseError as e:
                raise
            # hogy csak legelsőre menjen, ne írja be minden futáskor a fejlécet
            if ezres == 0:
                for i, elem in enumerate(cursor.description):
                    ugyf.write(0, i, elem[0])
            for r, row in enumerate(cursor.fetchall()):
                for c, col in enumerate(row):
                    ugyf.write(r + 1 + ezres * 999, c, col)

    print("Exportálás Excelbe...")
    workbook.close()

    print("Ügyféladat lekérdezés kész")


def timestamp():
    return datetime.now().strftime("%Y%m%d") + "_" + datetime.now().strftime("%H%M")


def lekerdezes(run):
    # Csatlakozás az adatbázishoz, hibát dob ha rossz a jelszó
    dsn_tns = cx_Oracle.makedsn("prac-scan-bar.khb.hu", 1521, service_name="POLTP_APP.khb.hu")
    try:
        db = cx_Oracle.connect(user=run.user, password=run.passw, dsn=dsn_tns)
    except cx_Oracle.DatabaseError as e:
        raise

    fromd_str = run.fromd.strftime("%Y/%m/%d")
    tod_str = run.tod.strftime("%Y/%m/%d")

    # timestampelt (perc a legkisebb érték) excelt nyit
    # külön kimentem, így később is rendelkezésre áll, hogy melyik fájl tartozik a munkamenethez
    run.stamp = timestamp()
    wexcel = run.hova + 'prod_transactions_' + run.stamp + '.xlsx'
    workbook = xlsxwriter.Workbook(wexcel, {'constant_memory': True, 'default_date_format': 'yyyy/mm/dd'})
    tranz = workbook.add_worksheet()
    with db.cursor() as cursor:
        with open(run.file_sql + 'raw_sales_pl_script.sql', 'r') as sql_file:
            sql_mod = sql_file.read()
            sql_mod = re.sub('between to_date\([\s\S]*?,', 'between to_date(:fromd,', sql_mod, 1)
            sql_mod = re.sub('and to_date\([\s\S]*?,', 'and to_date(:tod,', sql_mod, 1)
            if not bool(run.reta.get()):
                sql_mod = sql_mod.replace("AND TYPOLOGY LIKE ('%RETA%')", "--AND TYPOLOGY LIKE ('%RETA%')")
            try:
                cursor.execute(sql_mod, {'fromd': fromd_str, 'tod': tod_str})
            except cx_Oracle.DatabaseError as e:
                raise

        for i, elem in enumerate(cursor.description):
            tranz.write(0, i,elem[0])

        for r, row in enumerate(cursor.fetchall()):
            for c, col in enumerate(row):
                tranz.write(r+1, c, col)
    workbook.close()
    print("Tranzakció lekérdezés kész")


if __name__ == "__main__":
    # print(timestamp())
    root = Tk()
    root.title("Excel riportok v1.3")
    root.geometry("720x480")

    # A létrejövő paraméterhalmazunk pár alapértelmezett értéke: 2023.01.01 mint kezdő lekérdezés dátum, a mai nap
    # mint végdátum, és a C:-n az sql és a Riportok mappa mint alapértelmezett célok
    class Submitting:
        def __init__(self, passw=StringVar(), user=StringVar(), fromd=date(2023, 1, 1), tod=datetime.today(),
                     reta=IntVar(), file_sql=None, hova=None):
            self.passw = passw
            self.user = user
            self.fromd = fromd
            self.tod = tod
            self.reta = reta
            self.stamp = timestamp()
            if file_sql is None:
                self.file_sql = 'C:\\sql\\'
            else:
                self.file_sql = file_sql

            if hova is None:
                self.hova = 'C:\\Riportok\\'
                if not os.path.exists(self.hova):
                    os.makedirs(self.hova)
            else:
                self.hova = hova


    # a jelszo alapján csatlakozik a db-hez
    run1 = Submitting()

    info = Message(root, text='A program lehúzza a megadott dátum időszakából a tranzakciós adatokat az sql alapján, és a tranzakciókhoz tartozó ügyféladatokat.', aspect=1400)
    info.grid(row=1, column=0, columnspan=4, padx=5, pady=15)

    user_input = Entry(root, textvariable=run1.user)
    user_input.insert(0, os.getlogin())
    user_input.grid(row=2, column=1)

    # Amennyiben kapunk robotot a programhoz, plain text helyett valami egyszerű, visszafejthető kódként tárolhatná
    # a jelszót
    pass_input = Entry(root, show="*", textvariable=run1.passw)
    pass_input.insert(0, unobscure(b'eNoLKTJOLC4tqow3sTSyjDcwAgAwHAUm').decode())
    pass_input.grid(row=3, column=1)

    reta_checkb = Checkbutton(root, text="Csak RETA-s ügyletek kellenek?", variable=run1.reta, onvalue=1, offvalue=0, width=40)
    reta_checkb.grid(row=4, column=1)

    fromd_label = Label(root, text='Kérem a kezdődátumot!')
    fromd_label.grid(row=2, column=0)
    dentry_fromd = DateEntry(root, selectmode="month", date_pattern='yyyy/mm/dd')
    dentry_fromd.grid(row=3, column=0)
    dentry_fromd.set_date(run1.fromd)

    tod_label = Label(root, text='Kérem a végdátumot!')
    tod_label.grid(row=4, column=0)
    dentry_tod = DateEntry(root, selectmode="month", date_pattern='yyyy/mm/dd')
    dentry_tod.grid(row=5, column=0)
    dentry_tod.set_date(run1.tod)

    pb = ttk.Progressbar(root, orient='horizontal', mode='determinate', length=200)
    pb.grid(row=6, column=1, pady=20, padx=55)

    def gombnyomas():
        if run1.passw != unobscure(b'eNoLKTJOLC4tqow3sTSyjDcwAgAwHAUm').decode():
            run1.passw = run1.passw = run1.passw.get()
        if run1.user != os.getlogin():
            run1.user = run1.user.get()
        # run1.reta = run1.reta.get()

        run1.tod = dentry_tod.get_date()
        run1.fromd = dentry_fromd.get_date()
        db_ugyfel(run1, root, pb)

    button_ugyfeltb = Button(root, text='Ügyféltábla', padx=55, pady=5, command=gombnyomas)
    button_ugyfeltb.grid(row=5, column=1)

    def ask_sql():
        run1.file_sql = filedialog.askdirectory(initialdir=r"C:\sql", title="SQl-ek helye") + "\\"

    button_sql = Button(root, text='SQL fájlok helye', padx=35, pady=5, command=ask_sql)
    button_sql.grid(row=2, column=2)

    def ask_riport():
        run1.hova = filedialog.askdirectory(initialdir=r"C:\Riportok", title="Riportok helye") + "\\"

    button_riport = Button(root, text='Riportok mentésének helye', padx=5, pady=5, command=ask_riport)
    button_riport.grid(row=3, column=2)

    mainloop()
