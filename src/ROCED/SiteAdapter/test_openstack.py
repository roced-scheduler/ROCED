#!/usr/bin/env python3

import os
from keystoneauth1 import loading
from keystoneauth1 import session

import novaclient.client
import glanceclient.client
import neutronclient.v2_0.client
import cinderclient.client

def get_image(glance, name="Standard_CentOS_7.3_latest"):
    for image in glance.images.list():
        if image["name"] == name:
            print(image["id"])
            return image
    return None

def get_network(neutron, name="SNAT"):
    networks=neutron.list_networks(name=name)
    return (networks['networks'][0]['id'])


def create_disk(cinder, size=100, image_id='27298832-7a55-42e4-ae15-60ed87a79236'):
    return cinder.volumes.create(size=size, imageRef=image_id)

loader = loading.get_plugin_loader('password')
auth = loader.load_from_options(auth_url=os.getenv('OS_AUTH_URL'),
                                username=os.getenv('OS_USERNAME'),
                                password=os.getenv('OS_PASSWORD'),
                                user_domain_name=os.getenv('OS_USER_DOMAIN_NAME'),
                                project_name=os.getenv('OS_PROJECT_NAME'))

sess = session.Session(auth=auth)
VERSION = "2"

nova = novaclient.client.Client(VERSION, session=sess)
glance = glanceclient.client.Client(VERSION, session=sess)
neutron = neutronclient.v2_0.client.Client(session=sess)
cinder = cinderclient.client.Client(VERSION,session=sess)

#print(cinder.volumes.list())
#print("create volume")
#volume_id=create_disk(cinder,size=40, image_id="27298832-7a55-42e4-ae15-60ed87a79236").id
print(cinder.volumes.list())
volume_id = '3d9f3b3e-ed91-4c36-81bf-a3a3259da81b'
print("volume id: %s" % volume_id)
#print(cinder.volumes.list()[0].id)
block_dev_mapping = {'vda':volume_id+":::0"}
#block_dev_mapping_v2= [{ "source_type": "volume", "boot_index": 0, "volume_id" :volume_id , "volume_size" : 40, 'destination_type' : 'volume'}]
block_dev_mapping_v2= [{ "boot_index": 0, "volume_id" :volume_id, "destination_type": "volume", "source_type": "image"}]

#print(block_dev_mapping)
#print(nova.servers.list())
#print(nova.flavors.list())
fl=nova.flavors.find(name="c1.medium")
#vm_image = None
#
#print("networt id for SNAT:", get_network(neutron,name="SNAT"))
#get_image(glance, name="Standard_CentOS_7.3_latest")
#print("request VM")
nova.servers.create("schnepf-test", flavor=fl, image='27298832-7a55-42e4-ae15-60ed87a79236', vol='3d9f3b3e-ed91-4c36-81bf-a3a3259da81b', nics=[{"net-id":get_network(neutron, name="SNAT")}])

#nova.servers.create("schnepf-test", flavor=fl, image=get_image(glance,name="Standard_CentOS_7.3_latest"), nics=[{"net-id":get_network(neutron, name="SNAT")}], block_device_mapping_v2=block_dev_mapping_v2)
#nova.servers.create("schnepf-test", flavor=fl, image=get_image(glance,name="Standard_CentOS_7.3_latest"), nics=[{"net-id":get_network(neutron, name="SNAT")}])
#va=nova.servers.list(search_opts={'name': 'schnepf-test'})

#attachment = cinder.volumes.attach(volume_id, va[0].id, '/storage/', mode='rw')
#print(nova.servers.list())

