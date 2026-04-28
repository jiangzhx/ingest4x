-- example HTTP POST script which demonstrates setting the
-- HTTP method, body, and adding a header

wrk.method = "POST"
wrk.body = '{"appid":"APPID","xcontext":{"idfa":"example_idfa","idfv":"example_idfv","caid":"example_caid","caid2":"example_caid2","installid":"example_installid","os":"Ios","channelid":"example_channelid","model":"example_model","pkgname":"example_pkgname","arg1":"arg1","arg2":"arg2","arg3":"arg3","arg4":"arg4"},"xwhat":"custom_event","xwhen":1718255925637,"xwho":"someoneelse"}'
wrk.headers["Content-Type"] = "application/json"