import paho.mqtt.client as mqtt

import db

class DataReader:
    def __init__(self, conn, topic):
        self.conn = conn
        self.topic = topic
    
    def run(self):
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqttc.on_connect = self.on_connect
        mqttc.on_message = self.on_message
        mqttc.connect("mqtt.eclipseprojects.io", 1883, 60)

        # tämä pitää clientin päällä
        mqttc.loop_forever()

    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected with result code {reason_code}")

        # heti, kun yhteys on muodostettu, tilaamme topicin "$SYS/#
        # nyt kun tilaus on tehty,  broker tietää, että sen pitää lähettää kaikki tähän topiciin tulevat viestit myös meille
        
        client.subscribe(self.topic)
    
    # tämä funktio suoritetaan, kun mqtt broker on julkaissut 
    # johonkin meidän tilaamamme topiciin viestin
    def on_message(self, client, userdata, msg):
        # tämä on vain esimerkki siitä, miten data otetaan vastaan
        # oikeasti tässä kohtaa projektissa tieto pitää tallentaa tietokantaan,
        # jotta sitä voidaan käsitellä myöhemmin
        payload = msg.payload.decode()
        print(f'Topic: {msg.topic}: {payload}')
        with self.conn.cursor() as cur:
            cur.execute('INSERT INTO temperatures(temperature) VALUES(%s)', (payload, ))
            self.conn.commit()


if  __name__ == '__main__':
    with db.connect() as conn:
        reader = DataReader(conn, "$SYS/broker/load/sockets/1min")
        reader.run()