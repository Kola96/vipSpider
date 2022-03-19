import sqlite3

from flask import Flask, request, redirect
from flask import render_template


app = Flask('vipSpiderAdmin')


conn = sqlite3.connect('vipSpider.db', check_same_thread=False)
cursor = conn.cursor()


def re_connect_sqlite():
    global conn
    global cursor
    conn = sqlite3.connect('vipSpider.db', check_same_thread=False)
    cursor = conn.cursor()


def smart_sqlite_exec(sql):
    try:
        cursor.execute(sql)
    except sqlite3.ProgrammingError:
        re_connect_sqlite()
        cursor.execute(sql)
    conn.commit()


@app.route('/', methods=['GET'])
def admin():
    smart_sqlite_exec('SELECT id, keyword, brand_code FROM vip_keyword ORDER BY create_time DESC')
    keyword_pairs = cursor.fetchall()
    return render_template('index.html', keyword_pairs=keyword_pairs)


@app.route('/addKeyword', methods=['POST'])
def add_keyword():
    keyword = request.form.get('keyword')
    brand = request.form.get('brand')
    smart_sqlite_exec(f'INSERT INTO vip_keyword (`keyword`, `brand_code`) VALUES ("{keyword}", "{brand}")')
    return redirect('/')


@app.route('/delKeyword', methods=['GET'])
def del_keyword():
    kid = request.args.get('id')
    smart_sqlite_exec(f'DELETE FROM vip_keyword WHERE `id` = "{kid}"')
    return redirect('/')


if __name__ == '__main__':
    app.run('0.0.0.0', 8899)
