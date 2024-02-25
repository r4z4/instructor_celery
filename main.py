from flask import Flask, render_template, request, session, flash, redirect, \
    url_for, jsonify
from celery import Celery
import psycopg2 

app = Flask(__name__) 
app.config['CELERY_BROKER_URL'] = 'redis://:password@localhost:6379'
app.config['CELERY_RESULT_BACKEND'] = 'redis://:password@localhost:6379'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
# Connect to the database 
conn = psycopg2.connect(database="flask_reports", user="postgres", 
                        password="postgres", host="localhost", port="5432") 
  
# create a cursor 
# cur = conn.cursor() 
  
# # if you already have any table or not id doesnt matter this  
# # will create a products table for you. 
# cur.execute( 
#     '''CREATE TABLE IF NOT EXISTS products (id serial \ 
#     PRIMARY KEY, name varchar(100), price float);''') 
  
# # Insert some data into the table 
# cur.execute( 
#     '''INSERT INTO products (name, price) VALUES \ 
#     ('Apple', 1.99), ('Orange', 0.99), ('Banana', 0.59);''') 
  
# # commit the changes 
# conn.commit() 
  
# # close the cursor and connection 
# cur.close() 
# conn.close() 
from datetime import date, datetime
from typing import Optional
from flask import request
import psycopg
import random
from psycopg.rows import class_row
from pydantic import BaseModel
import instructor
from openai import OpenAI, AsyncOpenAI
from typing import List
import asyncio


class Product(BaseModel):
    id: int
    name: str
    labelid: int
    category: str
    gender: str
    currentlyactive: bool
    created: datetime
    updated: Optional[datetime]

class Color(BaseModel):
    id: int
    name: str
    rgb: str

class UserDetail(BaseModel):
    name: str
    age: int
  
@app.route('/') 
def index(): 
    print(request.method)
    # Enables `response_model`
    # client = instructor.patch(OpenAI())

    # user = client.chat.completions.create(
    #     model="gpt-3.5-turbo",
    #     response_model=UserDetail,
    #     messages=[
    #         {"role": "user", "content": "Extract Jason is 25 years old"},
    #     ],
    # )

    # print(user)
    # assert isinstance(user, UserDetail)
    # assert user.name == "Jason"
    # assert user.age == 25
    # Connect to the database 
    conn = psycopg.connect(dbname="flask_reports", 
                            user="postgres", 
                            password="postgres", 
                            host="localhost", port="5432") 
  
    # create a cursor 
    cur = conn.cursor() 

    colors_query = '''SELECT * FROM webshop.colors'''
    products_query = '''SELECT * FROM webshop.products'''
  
    # Select all products from the table 
    with conn.cursor(row_factory=class_row(Color)) as cur:
        cur.execute(colors_query) 
  
        # Fetch the data 
        data = cur.fetchall() 
    
        # close the cursor and connection 
        cur.close() 

    conn.close()

    print(data)
  
    return render_template('index.html', data=data) 

@app.route('/instructor')
async def instructor_fn(): 
    # enables `response_model` in create call
    # client = instructor.patch(
    #     OpenAI(
    #         base_url="http://localhost:11434/v1",
    #         api_key="ollama",  # required, but unused
    #     ),
    #     mode=instructor.Mode.JSON,
    # )
    dataset = [
        "Tell me about the Elixir programming language, some similar languages and some of its features.",
        "Tell me about the Ruby programming language, some similar languages and some of its features.",
        "Tell me about the Haskell programming language, some similar languages and some of its features.",
        "Tell me about the Scala programming language, some similar languages and some of its features.",
    ]

    async def as_completed():
        all_langs: List[ProgrammingLanguage] = []
        tasks_get_langs = [extract_language(text) for text in dataset]
        for lang in asyncio.as_completed(tasks_get_langs):
            l = await lang
            all_langs.append(l)
            print(f"Person has been extracted: {l}")
        return all_langs
    
    # resp = client.chat.completions.create(
    #     model="codellama",
    #     max_retries=2,
    #     messages=[
    #         {
    #             "role": "user",
    #             "content": "Tell me about the Elixir programming language, some similar languages and some of its features.",
    #         }
    #     ],
    #     response_model=ProgrammingLanguage,
    # )

    # assert isinstance(resp, ProgrammingLanguage)
    # print(resp.model_dump_json(indent=2))
  
    return render_template('language.html', data=await as_completed()) 
  
  
@app.route('/create', methods=['POST']) 
def create(): 
    conn = psycopg2.connect(dbname="flask_reports", 
                            user="postgres", 
                            password="postgres", 
                            host="localhost", port="5432") 
  
    cur = conn.cursor() 
  
    # Get the data from the form 
    name = request.form['name'] 
    price = request.form['price'] 
  
    # Insert the data into the table 
    cur.execute( 
        '''INSERT INTO products \ 
        (name, price) VALUES (%s, %s)''', 
        (name, price)) 
  
    # commit the changes 
    conn.commit() 
  
    # close the cursor and connection 
    cur.close() 
    conn.close() 
  
    return redirect(url_for('index')) 
  
  
@app.route('/update', methods=['POST']) 
def update(): 
    conn = psycopg2.connect(dbname="flask_reports", 
                            user="postgres", 
                            password="postgres", 
                            host="localhost", port="5432") 
  
    cur = conn.cursor() 
  
    # Get the data from the form 
    name = request.form['name'] 
    price = request.form['price'] 
    id = request.form['id'] 
  
    # Update the data in the table 
    cur.execute( 
        '''UPDATE products SET name=%s,\ 
        price=%s WHERE id=%s''', (name, price, id)) 
  
    # commit the changes 
    conn.commit() 
    return redirect(url_for('index')) 
  
  
@app.route('/delete', methods=['POST']) 
def delete(): 
    conn = psycopg2.connect(dbname="flask_reports", user="postgres", password="postgres", host="localhost", port="5432") 
    cur = conn.cursor() 
  
    # Get the data from the form 
    id = request.form['id'] 
  
    # Delete the data from the table 
    cur.execute('''DELETE FROM products WHERE id=%s''', (id,)) 
  
    # commit the changes 
    conn.commit() 
  
    # close the cursor and connection 
    cur.close() 
    conn.close() 
  
    return redirect(url_for('index')) 

@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}

@app.route('/celery-mail', methods=['GET', 'POST'])
def celery_mail():
    if request.method == 'GET':
        return render_template('index.html', email=session.get('email', ''))
    email = request.form['email']
    session['email'] = email

    # send the email
    email_data = {
        'subject': 'Hello from Flask',
        'to': email,
        'body': 'This is a test email sent from a background Celery task.'
    }
    if request.form['submit'] == 'Send':
        # send right away
        send_async_email.delay(email_data)
        flash('Sending email to {0}'.format(email))
    else:
        # send in one minute
        send_async_email.apply_async(args=[email_data], countdown=60)
        flash('An email will be sent to {0} in one minute'.format(email))

    return redirect(url_for('index'))

@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}

@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)

class ProgrammingLanguage(BaseModel):
    name: str
    similars: List[str]
    features: List[str]

async def extract_language(text: str) -> ProgrammingLanguage:
    aclient = instructor.patch(AsyncOpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama",  # required, but unused
    ),
    mode=instructor.Mode.JSON,
    )

    return await aclient.chat.completions.create(  
        model="codellama",
        max_retries=2,
        messages=[
            {"role": "user", "content": text},
        ],
        response_model=ProgrammingLanguage,
    )
  
  
if __name__ == '__main__': 
    app.run(debug=True) 