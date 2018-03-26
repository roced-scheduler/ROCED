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

image_id = '27298832-7a55-42e4-ae15-60ed87a79236'
network_id = 'b3e518bd-718b-4aa8-b1de-d8cd9bdefb3d'
fl = nova.flavors.find(name="c1.medium")
disk_size = 40

print("create volume")
volume_id = cinder.volumes.create(size=disk_size, imageRef=image_id).id
print("volume created: %s" % volume_id)
block_dev_mapping = {'vda':volume_id+":::0"}
block_dev_mapping_v2= [{ "boot_index": 0, "volume_id" :volume_id, "destination_type": "volume", "source_type": "volume"}]
print("create vm")
#nova.servers.create("test-vm", flavor=fl, image=image_id, volume=volume_id, nics=[{"net-id":network_id}])
# does not attach volume

#nova.servers.create("test-vm", flavor=fl, image=image_id, volume=volume_id, nics=[{"net-id":network_id}], block_device_mapping=block_dev_mapping)
# novaclient.exceptions.BadRequest: Using different block_device_mapping syntaxes is not allowed in the same request. (HTTP 400)

nova.servers.create("test-vm", flavor=fl, image=image_id, volume=volume_id, nics=[{"net-id":network_id}], block_device_mapping_v2=block_dev_mapping_v2)
# with source_type image: novaclient.exceptions.BadRequest: Block Device Mapping is Invalid: Mapping image to local is not supported. (HTTP 400)
# with source_type volume: novaclient.exceptions.NotFound: Volume null could not be found. (HTTP 404)


