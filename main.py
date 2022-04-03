# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from base64 import b64encode, b64decode

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
def encrpytion(password):
    cipher = password.encode()
    encoded_cipher = b64encode(cipher)
    return encoded_cipher

def decrpytion(encoded_cipher):
    decoded_cipher = b64decode(encoded_cipher)
    return decoded_cipher.decode("utf-8")



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    eresult=encrpytion('tiger')
    # dresult = decrpytion()
    print(eresult)
    # print(dresult)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
