Original: https://github.com/jameycai/emqx_plugin_kafk

Modified to work for our use cases.
README below also translated. 


emqx-plugin-template
====================

[emqx/emqx-plugin-template at emqx-v4 (github.com)](https://github.com/emqx/emqx-plugin-template/tree/emqx-v4) This is a template plugin for the EMQ X broker. 

Plugin Config
-------------

Each plugin should have a 'etc/{plugin_name}.conf|config' file to store application config.

Authentication and ACL
----------------------

```
emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/3, [Env]).
emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]).
```

Build the EMQX broker
-----------------
###### 1. Install the relevant dependent components first. This article is based on the CentOS 7.9 environment and installed by yum
````
  yum -y install gcc gcc-c++ cpp glibc glibc-devel glibc-headers kernel-devel kernel-headers make m4 ncurses ncurses-devel openssl openssl-devel openssl-libs zlib zlib-devel libselinux-devel xmlto perl git wget zip unzip gtk2- devel binutils-devel unixODBC unixODBC-devel
 
  Note: The version of openssl is lower than 1.1.1k, you need to install openssl through the source code openssl-1.1.1k.tar.gz
  Note: If you test the CoAP function, you need to install libcoap, the specific address is https://libcoap.net/install.html
````

###### 2. Prepare Erlang/OTP R23 environment (latest erlang OTP 23 should work)
````
    ## Please select the corresponding Erlang/OTP rpm installation package for installation according to the CPU architecture and operating system of the compilation server.
     
    (a). Download erlang from a trusted package repo, or
    
    (b). Build from the source https://www.erlang.org/downloads/23 and follow instructions.
    
    Note: If the installation above is not supported, you can also install it through Erlang source code. Erlang source code download address: hhttps://www.erlang.org/downloads/23
    Note: Check if rebar3 is installed rebar3 -v , if not, you need to install it.
````


###### 3. Download EMQX source code

 Official source code repository address, https://github.com/emqx/emqx/tree/v4.4.10
````
  https://github.com/emqx/emqx/archive/refs/tags/v4.4.10.tar.gz
````

##### It is recommended to directly download the complete EMQX source package (based on the official emqx v4.3.12 open source source + kafka plugin), download address (https://github.com/jameycai/emqx/tree/main-v4.3).

https://github.com/jameycai/emqx/releases



###### 4. Modify the EMQX file and add the kafka plugin configuration

 Modify the Makefile in the EMQX home directory and add the following lines
 ````
  export EMQX_EXTRA_PLUGINS=emqx_plugin_kafka
 ````

 Modify the lib-extra/plugins file in the EMQX directory, add the following line to the emqx_plugin_kafka plugin in erlang_plugins
````
   , {emqx_plugin_kafka, {git, "https://github.com/jameycai/emqx_plugin_kafka.git", {branch, "main"}}} #This warehouse is your own warehouse address, which is convenient for code modification and submission
````
  

Note: The above configuration, in the complete EMQX source package (based on the official emqx v4.3 open source source + kafka plug-in), already includes the relevant configuration and does not need to be configured.


###### 5. Compile EMQX source code and start EMQX
Enter the emqx-v4.3 source code directory and execute the make command. This process will stop due to network problems and repeated errors. You only need to keep making until it succeeds. Conditional advice is to use the Internet scientifically.
````
  Binary compilation command: make
  Docker image packaging: make emqx-docker

  After the compilation is successful, the _build directory will appear, then enter the _build/emqx/rel/emqx/bin directory, and start emqx, as follows:
  ./emqx start
````

Note: If the ports of the CoAP protocol and the Lwm2m protocol conflict, you need to set the ports of the two respectively.


###### 6. Start EMQX successfully, access the address ( http://ip:18083 ) through the browser, and access the console:

![image](https://user-images.githubusercontent.com/13848153/169473622-00443f97-b3ef-47cf-92eb-ef9cc06e9305.png)

![image](https://user-images.githubusercontent.com/13848153/169473900-c897e274-316d-4734-bc41-c1ddd15f83e5.png)

![image](https://user-images.githubusercontent.com/13848153/169473987-a6a97bc7-08ed-4943-a110-9bd23cdf390b.png)

Test MQTT to Kafka to receive messages
![image](https://user-images.githubusercontent.com/13848153/169672811-98ec0240-b5d5-4fdc-a4fe-a9082aeb6d15.png)


License
-------

Apache License Version 2.0

Author
------

EMQ X Team.
