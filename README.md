# Verificador de Preenchimento de Cadastro
Este script Python foi desenvolvido com o objetivo de verificar se um cadastro em uma plataforma está completamente preenchido ou se ainda faltam algumas informações necessárias. O propósito é criar uma ferramenta em POWER BI que permita monitorar a situação atual do cadastro de cada entidade em um banco de dados, visando identificar aquelas que não finalizaram o preenchimento de todas as informações.

## Funcionamento
**Conexão com o Banco de Dados:** O script estabelece uma conexão com um banco de dados SQL Server, utilizando as bibliotecas pyodbc e sqlalchemy.

**Importação da Tabela:** Uma tabela é importada do banco de dados para um DataFrame do Pandas.

**Extração do JSON:** Dados estruturados em formato JSON contidos na coluna "snapshot" são normalizados e armazenados em um novo DataFrame.

**Organização dos Dados:**

- Dados Gerais da Entidade: Informações básicas da entidade são extraídas do DataFrame principal.
- Dados de Endereço da Entidade: Detalhes de endereço são extraídos e normalizados.
- Dados dos Dirigentes: Informações sobre os dirigentes da entidade são extraídas e organizadas.
- Dados dos Usuários: Informações dos usuários associados à entidade são coletadas e organizadas.
- União dos Dados: Todos os conjuntos de dados são mesclados em um único DataFrame.

**Renomeação de Colunas:** As colunas são renomeadas para melhor clareza e consistência.

**Exclusão de Colunas Repetidas:** Colunas redundantes são removidas.

**Verificação de Dados Nulos:** É feita uma verificação para identificar quais campos estão nulos em cada linha.

**Criação da Coluna "colunas_nulas":** Uma nova coluna é criada para indicar quais campos estão nulos para cada linha.

**Tabela Final e Salvamento no Banco de Dados:** O DataFrame final, contendo apenas as colunas de identificação da entidade e a informação sobre campos nulos, é salvo de volta no banco de dados com o nome "tb_campos_nulos_cadastro_entidade".

## Utilização
Para utilizar este script, é necessário ter acesso ao banco de dados onde estão armazenados os cadastros das entidades. A conexão com o banco de dados deve ser configurada adequadamente no script, fornecendo o nome do servidor, o nome do banco de dados e o driver necessário.

Após a execução do script, a tabela resultante estará disponível no banco de dados, permitindo a análise da situação do cadastro de cada entidade.

*Nota: Certifique-se de que todas as dependências, como as bibliotecas Pandas, PyODBC e SQLAlchemy, estejam devidamente instaladas para executar o script com sucesso.*
