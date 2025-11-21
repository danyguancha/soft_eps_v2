"""
Generador de datos de prueba para performance tests
"""
import pandas as pd
from io import BytesIO
import random
from datetime import datetime, timedelta


def generate_csv_data(rows: int, columns: int = 15) -> BytesIO:
    """Genera CSV con datos sintéticos"""
    departamentos = ['CALDAS', 'ANTIOQUIA', 'VALLE', 'NARIÑO', 'CUNDINAMARCA']
    municipios = {
        'CALDAS': ['MANIZALES', 'CHINCHINA', 'VILLAMARIA'],
        'ANTIOQUIA': ['MEDELLIN', 'BELLO', 'ITAGUI'],
        'VALLE': ['CALI', 'PALMIRA', 'BUENAVENTURA'],
        'NARIÑO': ['PASTO', 'IPIALES', 'TUMACO'],
        'CUNDINAMARCA': ['BOGOTA', 'SOACHA', 'CHIA']
    }
    
    data = {
        'documento': [random.randint(1000000, 99999999) for _ in range(rows)],
        'nombre': [f"Persona {i}" for i in range(rows)],
        'fecha_nacimiento': [(datetime.now() - timedelta(days=random.randint(0, 1825))).strftime('%Y-%m-%d') for _ in range(rows)],
    }
    
    # Generar departamento y municipio coherentes
    dept_list = [random.choice(departamentos) for _ in range(rows)]
    data['departamento'] = dept_list
    data['municipio'] = [random.choice(municipios[dept]) for dept in dept_list]
    
    # Columnas adicionales
    for i in range(5, columns):
        data[f'columna_{i}'] = [f'Valor_{random.randint(1, 100)}' for _ in range(rows)]
    
    df = pd.DataFrame(data)
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    
    return buffer


def generate_excel_data(rows: int, sheets: int = 3, columns: int = 20) -> BytesIO:
    """Genera archivo Excel con múltiples hojas"""
    buffer = BytesIO()
    
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        for sheet_num in range(sheets):
            rows_per_sheet = rows // sheets
            data = {
                'documento': [random.randint(1000000, 99999999) for _ in range(rows_per_sheet)],
                'nombre': [f"Usuario {i}" for i in range(rows_per_sheet)],
                'edad': [random.randint(0, 5) for _ in range(rows_per_sheet)],
                'departamento': [random.choice(['CALDAS', 'ANTIOQUIA']) for _ in range(rows_per_sheet)],
            }
            
            for i in range(4, columns):
                data[f'col_{i}'] = [random.randint(1, 43000) for _ in range(rows_per_sheet)]
            
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name=f'Hoja{sheet_num + 1}', index=False)
    
    buffer.seek(0)
    return buffer


def generate_technical_note_data(rows: int = 43000) -> BytesIO:
    """
    Genera CSV de Technical Note con datos realistas para pruebas de rendimiento
    
    Args:
        rows: Numero de registros a generar (default: 43000)
    
    Returns:
        BytesIO: Buffer con el CSV generado
    """
    departamentos = {
        'CALDAS': ['MANIZALES', 'CHINCHINA', 'VILLAMARIA', 'PALESTINA', 'SUPIA'],
        'NARIÑO': ['IPIALES', 'PASTO', 'TUMACO', 'SAMANIEGO', 'CUMBAL'],
        'ANTIOQUIA': ['MEDELLIN', 'BELLO', 'ITAGUI', 'ENVIGADO', 'SABANETA'],
        'VALLE': ['CALI', 'PALMIRA', 'BUENAVENTURA', 'TULUA', 'CARTAGO']
    }
    
    ips_por_depto = {
        'CALDAS': [
            'HOSPITAL SANTA SOFIA',
            'CENTRO DE SALUD MANIZALES',
            'IPS CALDAS SALUD',
            'CLINICA VERSALLES'
        ],
        'NARIÑO': [
            'IPS INDIGENA MALLAMAS',
            'HOSPITAL SAN PEDRO',
            'CENTRO DE SALUD IPIALES',
            'IPS NARIÑO'
        ],
        'ANTIOQUIA': [
            'HOSPITAL SAN VICENTE',
            'IPS UNIVERSITARIA',
            'CENTRO SALUD BELLO',
            'METROSALUD'
        ],
        'VALLE': [
            'HOSPITAL UNIVERSITARIO',
            'CLINICA VALLE',
            'IPS VALLE SALUD',
            'CENTRO MEDICO CALI'
        ]
    }
    
    servicios = [
        'Medicina General',
        'Control Niño Sano',
        'Vacunacion',
        'Odontologia',
        'Nutricion',
        'Psicologia',
        'Enfermeria',
        'Consulta Externa'
    ]
    
    # Generar datos
    data = {
        'documento': [],
        'nombre': [],
        'fecha_nacimiento': [],
        'departamento': [],
        'municipio': [],
        'ips': [],
        'SERVICIO': []
    }
    
    base_year = 2020
    
    for i in range(rows):
        # Seleccionar departamento
        dept = random.choice(list(departamentos.keys()))
        
        # Generar datos coherentes
        data['documento'].append(random.randint(1000000, 99999999))
        data['nombre'].append(f"Paciente {i+1}")
        
        # Fecha de nacimiento entre 2020-2025 (niños 0-5 años)
        year = base_year + (i % 6)
        month = 1 + (i % 12)
        day = 1 + (i % 28)
        data['fecha_nacimiento'].append(f"{year}-{month:02d}-{day:02d}")
        
        data['departamento'].append(dept)
        data['municipio'].append(random.choice(departamentos[dept]))
        data['ips'].append(random.choice(ips_por_depto[dept]))
        data['SERVICIO'].append(random.choice(servicios))
    
    # Crear DataFrame
    df = pd.DataFrame(data)
    
    # Convertir a BytesIO
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    
    return buffer
