# lista as chaves de acesso aos bancos com base no secret.key gerado pela gen_file_key.py
from cryptography.fernet import Fernet

# Carregue a chave (assegure-se de que secret.key está no local correto)
with open('secret.key', 'rb') as key_file:
    key = key_file.read()
f = Fernet(key)

# Para Firebird:
password_firebird_clear = "masterkey"
password_firebird_encrypted = f.encrypt(password_firebird_clear.encode()).decode()
print(f"Senha Firebird criptografada: {password_firebird_encrypted}")
# Cole esta no config_firebird.json

# Para MySQL:
password_mysql_clear = "0305duxx"
password_mysql_encrypted = f.encrypt(password_mysql_clear.encode()).decode()
print(f"Senha MySQL criptografada: {password_mysql_encrypted}")
# Cole esta no config_mysql.json

# Para PostgreSQL:
password_postgres_clear = "0305duxx"
password_postgres_encrypted = f.encrypt(password_postgres_clear.encode()).decode()
print(f"Senha PostgreSQL criptografada: {password_postgres_encrypted}")
# Cole esta no config_postgres.json