import pandas as pd
import random

class Encryptor:
    def __init__(self, english_path: dict):
        """
        Initialize the Encryptor with a dictionary (dictionary) for validation.
        """
        self.english_path = english_path
        self.english_dict = {} 

    def encrypt(self, df, column) -> pd.DataFrame:
        """
        Encrypt the specified column in the DataFrame using a Caesar cipher with a random key.
        """
        self.encryption_key = self.generate_random_key()

        df[column] = df[column].apply(lambda x: self.caesar_cipher(x, self.encryption_key))

        return df
    
    def decrypt(self, df, column) -> pd.DataFrame:
        """
        Brute force decrypt the specified column in the DataFrame using Caesar cipher and dictionary validation.
        Decrypt only the first message, get the best shift key, and use it to decrypt the entire column.
        """
        first_message = df[column].iloc[0] 
        best_shift = self.get_best_shift(first_message) 
       

        df[column] = df[column].apply(lambda x: self.caesar_cipher(x, -best_shift))  

        return df

    def get_best_shift(self, encrypted_text: str) -> int:
        """
        Try all shifts (1-25) on the first message and find the shift that results in the most valid English words.
        Sum the values of the valid words in the dictionary and select the shift with the highest score.
        """
        best_shift = 0
        max_score = 0
        self.english_dict = self.get_english_dict(self.english_path)

        for shift in range(1, 25): 
            decrypted_text = self.caesar_cipher(encrypted_text, -shift) 

            words = decrypted_text.split() 
            score = sum(self.english_dict.get(word.lower(), 0) for word in words)

            if score > max_score:
                best_shift = shift
                max_score = score
        return best_shift

    def caesar_cipher(self, text: str, key: int) -> str:
        """
        Encrypts or decrypts the text using Caesar cipher with a given key (shift).
        """
        encrypted_text = []
        for char in text:
            if char.isalpha():
                shift = 65 if char.isupper() else 97
                encrypted_char = chr((ord(char) - shift + key) % 26 + shift)
                encrypted_text.append(encrypted_char)
            else:
                encrypted_text.append(char)
        return ''.join(encrypted_text)
    
    def generate_random_key(self) -> int:
        """
        Generates a random key (shift value) for encryption (between 1 and 25).
        """
        return random.randint(1, 25)
    
    def get_english_dict(self, english_path):
        english_dict = {}
        with open(english_path, 'r') as file:
            for line in file:
                words = line.strip().split(' ') 
                for word in words:
                    english_dict[word.lower()] = 1  
        return english_dict