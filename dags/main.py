from ast import Import
from distutils.ccompiler import gen_preprocess_options
from lib2to3.pgen2.pgen import generate_grammar
from tracemalloc import start
from unicodedata import name
from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import pandas as pd
from airflow.operators.email import EmailOperator

default_args={"owner":"airflow",}

def start():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    connection.run("MATCH (n) DETACH DELETE n;")
    connection.run("CALL gds.graph.drop('purchases',false)")

def Graph():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    connection.run("CREATE (dan:Person {name: 'Dan'}),(annie:Person {name: 'Annie'}),(matt:Person {name: 'Matt'}),(jeff:Person {name: 'Jeff'}),(brie:Person {name: 'Brie'}),(elsa:Person {name: 'Elsa'}),(cookies:Product {name: 'Cookies'}),(tomatoes:Product {name: 'Tomatoes'}),(cucumber:Product {name: 'Cucumber'}),(celery:Product {name: 'Celery'}),(kale:Product {name: 'Kale'}),(milk:Product {name: 'Milk'}),(chocolate:Product {name: 'Chocolate'}),(dan)-[:BUYS {amount: 1.2}]->(cookies),(dan)-[:BUYS {amount: 3.2}]->(milk),(dan)-[:BUYS {amount: 2.2}]->(chocolate),(annie)-[:BUYS {amount: 1.2}]->(cucumber),(annie)-[:BUYS {amount: 3.2}]->(milk),(annie)-[:BUYS {amount: 3.2}]->(tomatoes),(matt)-[:BUYS {amount: 3}]->(tomatoes),(matt)-[:BUYS {amount: 2}]->(kale),(matt)-[:BUYS {amount: 1}]->(cucumber),(jeff)-[:BUYS {amount: 3}]->(cookies),(jeff)-[:BUYS {amount: 2}]->(milk),(brie)-[:BUYS {amount: 1}]->(tomatoes),(brie)-[:BUYS {amount: 2}]->(milk),(brie)-[:BUYS {amount: 2}]->(kale),(brie)-[:BUYS {amount: 3}]->(cucumber),(brie)-[:BUYS {amount: 0.3}]->(celery),(elsa)-[:BUYS {amount: 3}]->(chocolate),(elsa)-[:BUYS {amount: 3}]->(milk);")

def oView():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    print(connection.run("MATCH ( p:Person) with count(p) as count return 'Number of people' as label, count"))
    print(connection.run("MATCH (a:Person)-[r:BUYS]->(b:Product) with count(r) as count return 'Number of RELATION' as label, count"))

def FastRP():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    connection.run("CALL gds.graph.project('purchases',['Person','Product'],{BUYS: {orientation: 'UNDIRECTED', properties: 'amount'}})")
    connection.run("CALL gds.fastRP.mutate('purchases',{embeddingDimension: 4,randomSeed: 42,mutateProperty: 'embedding',relationshipWeightProperty: 'amount',iterationWeights: [0.8, 1, 1, 1]})YIELD nodePropertiesWritten")

def KNN():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    connection.run("CALL gds.knn.write('purchases', {topK: 2,nodeProperties: ['embedding'],randomSeed: 42,concurrency: 1,sampleRate: 1.0,deltaThreshold: 0.0,writeRelationshipType: 'SIMILAR',writeProperty: 'score'})")

def save():
    Info = pd.DataFrame()
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    Info = connection.run("MATCH (n:Person)-[r:SIMILAR]->(m:Person) RETURN n.name as person1, m.name as person2, r.score as similarity ORDER BY similarity DESCENDING, person1, person2")
    return Info

def make_racc():
    connection=Neo4jHook(conn_id="neo4j_conn_id")
    return connection.run("MATCH (:Person {name: 'Annie'})-->(p1:Product) WITH collect(p1) as products MATCH (:Person {name: 'Matt'})-->(p2:Product) WHERE not p2 in products RETURN p2.name as recommendation")

with DAG(
    dag_id="Workflow",
    start_date=datetime(2022,5,16),
    schedule_interval="@daily",
    default_args=default_args)as dag:

    Starter = PythonOperator(
        task_id='Starter',
        python_callable= start
    )

    Graph_Creation = PythonOperator(
        task_id='Graph_Creation',
        python_callable= Graph
    )

  
    Overview = PythonOperator(
        task_id='Overview',
        python_callable= oView
    )

    Embedding = PythonOperator(
        task_id='Embedding',
        python_callable= FastRP
    )

    Knn = PythonOperator(
        task_id='Knn',
        python_callable= KNN
    )

    Save_Data = PythonOperator(
        task_id='Save_Data',
        python_callable= save
    )

    Raccomandation = PythonOperator(
        task_id='Raccomandation',
        python_callable= make_racc
    )



Starter >> Graph_Creation >> Overview >> Embedding >> Knn >> [Save_Data,Raccomandation]