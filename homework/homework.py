"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
from pathlib import Path
import zipfile
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    
    # Constantes y rutas
    INPUT_DIR = Path("./files/input/")
    OUTPUT_DIR = Path("./files/output/")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Lectura de archivos
    raw_dataframes = []
    for zip_path in INPUT_DIR.glob("*.zip"):
        with zipfile.ZipFile(zip_path) as zip_file:
            for csv_name in zip_file.namelist():
                if csv_name.endswith(".csv"):
                    raw_dataframes.append(pd.read_csv(zip_file.open(csv_name)))
    
    raw_data = pd.concat(raw_dataframes, ignore_index=True)

    # Procesar datos de clientes
    CLIENT_COLUMNS = ['client_id', 'age', 'job', 'marital', 'education', 
                    'credit_default', 'mortgage']
    
    clients_data = raw_data[CLIENT_COLUMNS].copy()
    clients_data['job'] = (clients_data['job']
                        .str.replace('.', '', regex=False)
                        .str.replace('-', '_', regex=False))
    
    clients_data['education'] = (clients_data['education']
                            .str.replace('.', '_', regex=False)
                            .replace('unknown', pd.NA))
    
    binary_columns = ['credit_default', 'mortgage']
    for col in binary_columns:
        clients_data[col] = (clients_data[col] == 'yes').astype(int)

    # Procesar datos de campaña
    CAMPAIGN_COLUMNS = ['client_id', 'number_contacts', 'contact_duration',
                    'previous_campaign_contacts', 'previous_outcome', 
                    'campaign_outcome', 'day', 'month']
    
    campaign_data = raw_data[CAMPAIGN_COLUMNS].copy()
    campaign_data['previous_outcome'] = (campaign_data['previous_outcome'] == 'success').astype(int)
    campaign_data['campaign_outcome'] = (campaign_data['campaign_outcome'] == 'yes').astype(int)
    
    campaign_data['last_contact_date'] = pd.to_datetime(
        campaign_data['day'].astype(str) + '-' + 
        campaign_data['month'] + '-2022', 
        format='%d-%b-%Y'
    ).dt.strftime('%Y-%m-%d')
    
    campaign_data = campaign_data.drop(columns=['day', 'month'])

    # Procesar datos económicos
    ECONOMIC_COLUMNS = ['client_id', 'cons_price_idx', 'euribor_three_months']
    economic_data = raw_data[ECONOMIC_COLUMNS].copy()

    # Guardar resultados
    clients_data.to_csv(OUTPUT_DIR / "client.csv", index=False)
    campaign_data.to_csv(OUTPUT_DIR / "campaign.csv", index=False)
    economic_data.to_csv(OUTPUT_DIR / "economics.csv", index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()
