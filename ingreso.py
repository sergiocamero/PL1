import random

# Lista de matrículas españolas aleatorias (formato 0000 ABC)
matriculas = [f"{random.randint(0, 9999):04d} {random.choice('BCDFGHJKLMNPRSTVWXYZ')}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))}" for _ in range(10000)]

# Lista de 10,000 empresas aleatorias
empresas = ['Empresa' + str(i) for i in range(1, 10001)]
# Agregar UPS a la lista de empresas
empresas.append('UPS')

matriculas_generadas = set()

# Generar 20 millones de registros
num_registros = 20000000

with open('C:\\Users\\scamero\\Desktop\\UAH\\B.Datos2\\registros_camiones.txt', 'w') as file:
    for id_camion, _ in enumerate(range(num_registros), start=1):
        while True:
            matricula = f"{random.randint(0, 9999):04d} {random.choice('BCDFGHJKLMNPRSTVWXYZ')}{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))}"
            if matricula not in matriculas_generadas:
                matriculas_generadas.add(matricula)
                break
        empresa = random.choice(empresas)
        # Generar kilómetros aleatorios entre 0 y 500,000 km
        kilometros = random.randint(0, 500000)
        # Escribir registro en el archivo
        file.write(f"{id_camion},{matricula},{empresa},{kilometros}\n")

print("Archivo generado exitosamente.")
