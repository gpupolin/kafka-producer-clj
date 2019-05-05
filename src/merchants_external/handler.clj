(ns merchants-external.handler
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [environ.core :refer [env]]
            [ring.middleware.json :as middleware]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]))

(def producer-topic "merchant-external-test")
(def bootstrap-server (env :bootstrap-server "localhost:9092"))
(def zookeeper-hosts (env :zookeeper-hosts "localhost:2181"))

(defn- build-producer
  "Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer" StringSerializer
                        "key.serializer" StringSerializer
                        "bootstrap.servers" bootstrap-server}]
    (KafkaProducer. producer-props)))

(def producer (build-producer bootstrap-server))

(defn send
  "Send a kafka message"
  [key message]
  (.send producer (ProducerRecord. producer-topic key message))
)

(defn send-messages
  "Send a list of messages to Kafka"
  [request]
  (let [messages (get-in request [:body :merchants])]
          (doseq [message messages]
             (send (:key message) (:value message))
            )
        )
)

(defroutes app-routes
  (GET "/" [] 
       "Merchants External")
  (POST "/merchants-external" request
       (send-messages request))
  (route/not-found "Not Found"))

(def app
  (-> (wrap-defaults app-routes (assoc-in site-defaults [:security :anti-forgery] false))
      (middleware/wrap-json-body {:keywords? true})
      (middleware/wrap-json-response) )
)

