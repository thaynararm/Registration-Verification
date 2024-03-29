import pandas as pd
import pyodbc
import sqlalchemy
import json


server_name = 'nome_servidor'
database_name = 'nome_banco_de_dados'
driver = 'ODBC+Driver+17+for+SQL+Server'
conn_str = f'mssql://@{server_name}/{database_name}?driver={driver}'

engine = sqlalchemy.create_engine(conn_str)

#Importa a tabela
sql_query = "SELECT * FROM teste"
df = pd.read_sql_query(sql_query, engine)


#Extração do json
json_df = pd.json_normalize(df['snapshot'].apply(json.loads))


#Dados gerais da entidade
df_entidade = json_df[['id', 'ds_nome', 'dt_data_expedicao_cnpj', 'ds_nome_fantasia', 'ds_email']]


#Dados de endereço da entidade
endereco = pd.DataFrame(json_df['endereco'])
df_endereco = pd.json_normalize(endereco['endereco'].explode())
df_endereco = df_endereco[['pivot.fk_entidade', 'ds_cep', 'ds_rua']]


#Dados dos dirigentes
dirigentes = pd.DataFrame(json_df['dirigentes'])
df_dirigentes = pd.json_normalize(dirigentes['dirigentes'].explode())
df_dirigentes = df_dirigentes[[
    'pivot.fk_entidade',
    'ds_nome', 
    'bt_ativo', 
    'ds_estado_civil', 
    'ds_gender', 
    'ds_profissao', 
    'ds_cpf', 
    'ds_nacionalidade', 
    'ds_rg', 
    'ds_naturalidade', 
    'ds_uf', 
    'ds_orgao_emissor', 
    'ds_rg_uf',
    'endereco.ds_cep',
    'endereco.ds_rua', 
    'endereco.ds_numero',
    'contato.ds_telefone',
    'ds_email',
    'dt_inicio_mandato',
    'dt_fim_mandato',
    'cargo.ds_codigo',
    'cargo.ds_descricao',
    'bt_assinatura_entidade',
    'bt_assinatura_requerida',
    'bt_representante_legal']]


#Dados dos usuários
usuarios = pd.DataFrame(json_df['usuarios'])
df_usuarios = pd.json_normalize(usuarios['usuarios'].explode())
df_usuarios = df_usuarios[['fk_entidade','ds_nome_usuario', 'ds_cpf', 'ds_login', 'ds_telefone', 'ds_email', 'bt_ativo']]


#União de todos os dados
df_merged = pd.merge(df_entidade, df_endereco, left_on='id', right_on='pivot.fk_entidade', how='inner')
df_merged = pd.merge(df_merged, df_dirigentes, left_on='id', right_on='pivot.fk_entidade', how='inner')
df_merged = pd.merge(df_merged, df_usuarios, left_on='id', right_on='fk_entidade', how='inner')
df_merged = pd.DataFrame(df_merged)

#Renomeação das colunas
df_merged.columns = ['id_entidade', 'ds_nome_entidade', 'dt_data_expedicao_cnpj_entidade', 'ds_nome_fantasia_entidade', 'ds_email_entidade', 'pivot.fk_entidade__endereco', 'ds_cep_entidade', 'ds_rua_entidade', 'pivot.fk_entidade_dirigente', 'ds_nome_dirigente', 'bt_ativo_dirigente', 'ds_estado_civil_dirigente', 'ds_gender_dirigente', 'ds_profissao_dirigente', 'ds_cpf_dirigente', 'ds_nacionalidade_dirigente', 'ds_rg_dirigente', 'ds_naturalidade_dirigente', 'ds_uf_dirigente', 'ds_orgao_emissor_dirigente', 'ds_rg_uf_dirigente', 'endereco.ds_cep_dirigente', 'endereco.ds_rua_dirigente', 'endereco.ds_numero_dirigente', 'contato.ds_telefone_dirigente', 'ds_email_dirigente','dt_inicio_mandato_dirigente', 'dt_fim_mandato_dirigente', 'cargo.ds_codigo_dirigente', 'cargo.ds_descricao_dirigente', 'bt_assinatura_entidade_dirigente', 'bt_assinatura_requerida_dirigente', 'bt_representante_legal_dirigente', 'fk_entidade_usuarios', 'ds_nome_usuario', 'ds_cpf_usuario', 'ds_login_usuario', 'ds_telefone_usuario', 'ds_email_usuario', 'bt_ativo_usuario']


#Exclusão de colunas repetidas
df_merged = df_merged[['id_entidade', 'ds_nome_entidade', 'dt_data_expedicao_cnpj_entidade', 'ds_nome_fantasia_entidade','ds_email_entidade', 'ds_cep_entidade', 'ds_rua_entidade', 'ds_nome_dirigente', 'bt_ativo_dirigente', 'ds_estado_civil_dirigente', 'ds_gender_dirigente', 'ds_profissao_dirigente', 'ds_cpf_dirigente', 'ds_nacionalidade_dirigente', 'ds_rg_dirigente', 'ds_naturalidade_dirigente', 'ds_uf_dirigente', 'ds_orgao_emissor_dirigente', 'ds_rg_uf_dirigente', 'endereco.ds_cep_dirigente', 'endereco.ds_rua_dirigente',
    'endereco.ds_numero_dirigente', 'contato.ds_telefone_dirigente', 'ds_email_dirigente', 'dt_inicio_mandato_dirigente', 'dt_fim_mandato_dirigente', 'cargo.ds_codigo_dirigente',
    'cargo.ds_descricao_dirigente', 'bt_assinatura_entidade_dirigente', 'bt_assinatura_requerida_dirigente', 'bt_representante_legal_dirigente', 'ds_nome_usuario', 'ds_cpf_usuario', 'ds_login_usuario', 'ds_telefone_usuario', 'ds_email_usuario', 'bt_ativo_usuario']]


#Verifica se o nome de usuário é diferente do nome da entidade
df_merged = pd.DataFrame(df_merged[df_merged['ds_nome_entidade'] != df_merged['ds_nome_usuario']])

#Verifica quais campos são nulos
colunas_com_valores_nulos = df_merged.columns[df_merged.isnull().any()].tolist()


# Verifica se todas as colunas estão preenchidas para cada linha
colunas_preenchidas = df_merged.apply(lambda x: all(x.notna()), axis=1)

# Cria a coluna "colunas_nulas"
df_merged['colunas_nulas'] = df_merged.apply(lambda x: 'dados completos' if colunas_preenchidas[x.name] else ', '.join(col for col in colunas_com_valores_nulos if pd.isnull(x[col])), axis=1)


#Tabela final
df_merged = pd.DataFrame(df_merged[['id_entidade', 'colunas_nulas']])

# Converter para string apenas se não for do tipo string
df_merged['colunas_nulas'] = df_merged['colunas_nulas'].apply(lambda x: str(x) if not isinstance(x, str) else x)


# Salve o DataFrame no SQL Server
df_merged.to_sql(name='tb_campos_nulos_cadastro_entidade', con=engine, if_exists='replace', index=False)

print('Tabela salva no SQL Server com sucesso!')