(defproject merchants-external "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [environ "1.1.0"]
                 [compojure "1.6.1"]
                 [ring/ring-json "0.4.0"]
                 [org.apache.kafka/kafka-clients "2.0.0"]
                 [org.apache.kafka/kafka_2.12 "2.0.0"]
                 [ring/ring-defaults "0.3.2"]]
  :plugins [[lein-ring "0.12.5"]]
  :ring {:handler merchants-external.handler/app}
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.2"]]}})
