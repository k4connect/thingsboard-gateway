@ECHO OFF

@ECHO Stopping tb-gateway ...
net stop tb-gateway

@ECHO Uninstalling tb-gateway ...
%~dp0tb-gateway.exe uninstall

@ECHO DONE.