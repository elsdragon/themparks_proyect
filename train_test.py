from sklearn.model_selection import train_test_split
import pandas as pd
data_dlp = pd.read_csv('dlp_dataclean.csv')
# Separación del data set en training y testing

dlp_tr, dlp_test = train_test_split(data_dlp, test_size=0.2, shuffle=True, random_state=42)
dlp_train, dlp_val = train_test_split(dlp_tr, test_size=0.2, shuffle=True, random_state=42)
# Comprobamos la dimensionalidad de los dos dataset
print(f'Dimensiones del dataset de training: {dlp_train.shape}')
print(f'Dimensiones del dataset de validation: {dlp_val.shape}')
print(f'Dimensiones del dataset de test: {dlp_test.shape}')
# Guardamos Training y testing en csv separados

dlp_train.to_csv('dlp_train.csv', sep=';', decimal='.', index=False)
dlp_val.to_csv('dlp_validation.csv', sep=';', decimal='.', index=False)
dlp_test.to_csv('dlp_test.csv', sep=';', decimal='.', index=False)