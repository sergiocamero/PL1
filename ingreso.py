import random

# Lista de 10,000 empresas aleatorias
empresas = ['Empresa' + str(i) for i in range(1, 10001)]
# Agregar UPS a la lista de empresas
empresas.append('UPS')

# Generar 20 millones de registros
num_registros = 20000000

with open('C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt', 'w') as file:
    for _ in range(num_registros):
        empresa = random.choice(empresas)
        # Generar kilómetros aleatorios entre 0 y 500,000 km
        kilometros = random.randint(0, 500000)
        # Escribir registro en el archivo
        file.write(f"{empresa},{kilometros}\n")

print("Archivo generado exitosamente.")
