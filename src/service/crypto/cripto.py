from cryptography.fernet import Fernet

def criptar():
    key = Fernet.generate_key()
    print(key)
    cipher_suite = Fernet(key)
    print(cipher_suite)
    encoded_text = cipher_suite.encrypt(b"TESTE")
    print(encoded_text)
    decoded_text = cipher_suite.decrypt(encoded_text)
    print(decoded_text)


if __name__ == '__main__':
    try:
        criptar()
    except Exception as e:
        print('Error: ' + str(e))