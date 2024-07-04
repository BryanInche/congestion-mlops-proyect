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
        SELECT tz.congestion, tz.eq_id, tz.x, tz.y, tz.z, tz.bearing_t, tz.speed_t, tz.gear_t, tz.pitch_t, tz.roll_t, 
        tz.n_sat, tz.tramosidsnew_t, tz.id_trabaj_t, tz.tempeje1_t, tz.tempeje2_t, tz.tempeje3_t,tz.tempeje4_t, tz.tempeje5_t, 
        tz.tempeje6_t, tz.presllanta1_t, tz.presllanta2_t, tz.presllanta3_t, tz.presllanta4_t, tz.presllanta5_t, tz.presllanta6_t, 
        tz.isload_t, tz.tonelaje_t, tz.marcha_t, tz.latitude_t, tz.longitud_t, tz.precisiongps_t, tz.direccion_t,tz.combustibleint_t,
        tz.frecuencia_t, tz.templlanta1_t, tz.templlanta2_t, tz.templlanta3_t, tz.templlanta4_t, tz.templlanta5_t, tz.templlanta6_t, 
        tz.bateriasensorllanta1_t, tz.bateriasensorllanta2_t, tz.bateriasensorllanta3_t, tz.bateriasensorllanta4_t, 
        tz.bateriasensorllanta5_t, tz.bateriasensorllanta6_t, tz.segment_angle_t, tz.fuel_rate_t,tz.instant_date_t,  
        tz.id_tatramosstat, tz.id_nodoini, tz.id_nodofin, tz.nombre_tramo, tz.limitevelcod, tz.limitevelcdo, tz.limiteveldod, 
        tz.limitevelddo, tz.id_coormapaini, tz.id_coormapafin, tz.anchoizq, tz.anchoder, tz.peralteizq, tz.peralteder, 
        tz.isunderground, tz.height, tz.sidenumber, tz.marchacod, tz.marchacdo, tz.marchadod, tz.marchaddo, tz.adelantarod, 
        tz.adelantardo, tz.id_category_path, tz.id_equipment, tz.id_path, tz.id_worker, tz.message, tz.start_xcoorint,
        tz.start_ycoorint, tz.start_zcoorint, tz.end_xcoorint, tz.end_ycoorint, tz.end_zcoorint, tz.alert_level, tz.start_time_alert,
        tz.end_time_alert, tz.is_viewed, tz.id_user_viewed, tz.id_equipment_congestion,
        e.id_equipo, e.nombre FROM (
        SELECT c.id is not null congestion, *
        FROM public.getsensorsvaluesmod(%s, '2024-03-27 00:01:30.612-05', '2024-04-23 23:59:30.612-05') a --data_camion
        left join tp_tramosstat b
        on b.id = (select id from tp_tramosstat where id_tptramosstat = a.tramosidsnew_t and tiem_creac < a.instant_date_t
                                order by tiem_creac desc limit 1)
        left join congestion_alerts c
        on c.id = (select id from congestion_alerts where id_equipment_congestion = a.eq_id and start_time_alert >= a.instant_date_t 
                        and end_time_alert > a.instant_date_t order by start_time_alert limit 1)) tz
        left join ts_equipos e
        on tz.eq_id = e.id_equipo
        WHERE tz.alert_level = 2 or tz.alert_level IS NULL
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
    
