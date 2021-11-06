from typing import Any, Dict, List
from sqlalchemy import *
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ArgumentError, InterfaceError, OperationalError
from sqlalchemy.orm import relationship

Base = declarative_base()

class Machine(Base):
    __tablename__ = "machine"
    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_name = Column(VARCHAR(256))
    memory       = Column(VARCHAR(256))
    cpu          = Column(Integer)
    storage      = Column(VARCHAR(256))
    processor    = Column(VARCHAR(256))
    colection_data = Column(DateTime)
    systems_machine = relationship("SystemMachine", back_populates="machine")
    

class SystemMachine(Base):
    __tablename__ = "system_machine"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    machine_id  = Column(Integer, ForeignKey('machine.id'))
    price       = Column(Numeric)
    region      = Column(VARCHAR(256))
    system_name = Column(VARCHAR(256))
    machine     = relationship("Machine", back_populates="systems_machine")

class SQL:

    # CONSTRUTOR DA CLASSE SQL
    def __init__(self, user=None, password=None, host=None, database_name=None):
        """Contrutor recebe os paramentros para conectar com o banco de dados, ao instaciar a classe SQL, conecta-se ao banco de dados, com as informações
        passadas por parametro."""

        # INFORMAÇÕES DO BANCO DE DADOS PARA CONECTAR AO BANCO
        self.host = host
        self.user = user
        self.password = password
        #Conexão utilizando mysql
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database_name}?charset=utf8mb4")
        self.conn = self.engine.connect()

        self.metadata = MetaData(bind=self.conn)
        self.session = Session(bind=self.conn)



    # DESTRUTOR DA CLASSE SQL
    def __del__(self):
        """Desconetar do banco de dados"""
        self.conn.close()
    
    def insert_in_bulk_machine(self,list_machine: List[Dict[str, Any]]) -> List[Any]:
        """ Insert the machines in bulk
            
            Args: 
                list_machine: List dictioranires of machine thal will insert on database
            Return:
                The list of dictionaries of machine with machine id of database

        """
        self.session.bulk_insert_mappings(Machine, list_machine, return_defaults=True) #RETORNAR CHAVE QUANDO INSERIR
        self.session.commit()
        return list_machine

    def insert_in_bulk_system_machine(self,list_system_machine: List[Dict[str, Any]]) -> List[Any]:
        """ Insert the systems of machines in bulk
            
            Args: 
                list_system_machine: List dictioranires of system machine thal will insert on database
            Return:
                The list of dictionaries of machine with machine id of database

        """
        self.session.bulk_insert_mappings(SystemMachine, list_system_machine, return_defaults=True) #RETORNAR CHAVE QUANDO INSERIR
        self.session.commit()
        return list_system_machine

    def create_tables_database(self):
        """Create all tables of database"""
        Base.metadata.create_all(self.engine)






    
    
