import paho.mqtt.client as mqtt

import db



# tämä funktio suoritetaan, kun python (paho-mqtt) koodimme
# saa yhteyden serverille
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")

    # heti, kun yhteys on muodostettu, tilaamme topicin "$SYS/#
    # nyt kun tilaus on tehty,  broker tietää, että sen pitää lähettää kaikki tähän topiciin tulevat viestit myös meille
    
    client.subscribe("$SYS/broker/load/sockets/1min")
    

# tämä funktio suoritetaan, kun mqtt broker on julkaissut 
# johonkin meidän tilaamamme topiciin viestin
def on_message(client, userdata, msg):
    # tämä on vain esimerkki siitä, miten data otetaan vastaan
    # oikeasti tässä kohtaa projektissa tieto pitää tallentaa tietokantaan,
    # jotta sitä voidaan käsitellä myöhemmin
    payload = msg.payload.decode()
    print(f'Topic: {msg.topic}: {payload}')
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO temperatures(temperature) VALUES(%s)', (payload, ))
            conn.commit()
    
       

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message

# mqtt.ecplipseprojects.io on julkinen broker
# 1883 on suojaamattoman mqqt-yhteyden portti
# 60 timeout. Jos asiakas ei saa brokerilta minuuttiin mitään viestejä
# asiakas itse lähettää PING-viestin brokerille, jotta yhteys pysyy auki
mqttc.connect("mqtt.eclipseprojects.io", 1883, 60)

# tämä pitää clientin päällä
mqttc.loop_forever()