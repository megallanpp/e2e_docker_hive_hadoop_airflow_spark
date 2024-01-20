import sqlalchemy as sa

from sqlalchemy.orm import sessionmaker

from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy.future.engine import Engine

#Variavel global para criar a sessão do BD
__engine: Optional[Engine] = None

"""
Função para criar a conexão ao banco de dados. 
"""

def create_engine_sqlserver(sql_server: bool = False):
    global __engine

    if __engine:
        return
    
    if sql_server:  
        conn_str = "mssql+pyodbc://userLeitura:Leitura%40Cotra2021@srv01-db.cpou2imhjiqn.sa-east-1.rds.amazonaws.com/TesteDW?driver=ODBC+Driver+17+for+SQL+Server"
        __engine = sa.create_engine(url=conn_str, echo=False)     
    return __engine


"""
Função para criar a sessão ao banco de dados. 
"""


def create_session_sqlserver() -> Session:
    global __engine


    if not __engine:
        create_engine_sqlserver(sql_server=True) # create_engine(sqlite=True)
    
    __session = sessionmaker(__engine, expire_on_commit=False, class_=Session)

    session: Session = __session()

    return session

