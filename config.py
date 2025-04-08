config = {
'bootstrap.servers':'<bootstrap here>',
'security.protocol':'SASL_SSL',
'sasl.mechanisms':'PLAIN',
'sasl.username':'<api key here>',
'sasl.password':'<api secret here>',
'client.id':'GameTelemetryServer-Producer'
}

sr_config = {
    'url': '<schema registry URL here>',
    'basic.auth.user.info':'<SR key here>:<SR secret here>'
}