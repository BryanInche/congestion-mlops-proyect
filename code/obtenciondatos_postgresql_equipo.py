import psycopg2
import pandas as pd

def consultar_postgres_y_obtener_df_c4m(parametro):
    host = "srv-postgres-d4m.postgres.database.azure.com"
    database = "ControlSenseDB"
    user = "administrador"
    password = "Protobuffers2024"
    # Establecer la conexión a la base de datos
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

        # Crear un cursor para ejecutar comandos SQL
        cursor = connection.cursor()

        # Establecer la zona horaria antes de ejecutar la consulta
        time_zone_query = "SET TIME ZONE 'America/Lima';"
        cursor.execute(time_zone_query)

        # Tu consulta SQL con el parámetro parametro
        tu_query_sql = '''
        select main.*, e.id_equipo, e.nombre from (SELECT *
        FROM public.getsensorsvaluesmod(%s, '2024-02-27 07:00:30.612-05', '2024-03-03 06:27:30.612-05') a --data_camion
        left join tp_tramosstat b
        on b.id = (select id from tp_tramosstat where id_tptramosstat = a.tramosidsnew_t and tiem_creac < a.instant_date_t
                        order by tiem_creac desc limit 1)) main
        left join ts_equipos e
        on main.eq_id = e.id_equipo				
        where main.nombre_tramo is not null
        order by main.instant_date_t asc
        '''

        # Ejecutar la consulta con el parámetro parametro
        cursor.execute(tu_query_sql, (parametro,))

        # Obtener los resultados en un DataFrame de pandas
        resultados_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        # Cerrar el cursor y la conexión
        cursor.close()
        connection.close()

        # Devolver el DataFrame con los resultados
        return resultados_df

    except psycopg2.Error as e:
        print("Error al conectar a la base de datos PostgreSQL:", e)
        return None
    
