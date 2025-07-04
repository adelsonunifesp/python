# gera o arquivo de key que será utilizado em list_key.py
from cryptography.fernet import Fernet

key = Fernet.generate_key()
with open('secret.key', 'wb') as key_file:
    key_file.write(key)
print("Chave 'secret.key' gerada com sucesso!")