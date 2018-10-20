#!/usr/bin/env python

import sys
import os
import pwd
import time
from Pegasus.DAX3 import *

# The name of the DAX file is the first argument
if len(sys.argv) != 2:
   sys.stderr.write("Usage: %s DAXFILE\n" % sys.argv[0])
   sys.exit(1)

daxfile = sys.argv[1]
USER = pwd.getpwuid(os.getuid())[0]

dax = ADAG("casa_wf")
dax.metadata("name", "CASA")
dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
dax.metadata("created", time.ctime())

input_netcdf = File("netcdf_input")
maxVelocity_netcdf = File("maxVelocity.netcdf")

vel_job = Job("um_vel")
vel_job.addArguments(netcdf)
vel_job.uses(input_netcdf, link=Link.INPUT)
vel_job.uses(maxVelocity_netcdf, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(vel_job)

colorscale = File("max_wind.png")
maxVelocityImg = File("maxVelocity.png")

post_vel_job = Job("merged_netcdf2png")
post_vel_job.addArguments("-c", colorscale, "-q 235 -z 11.176,38", "-o", maxVelocityImg, maxVelocity_netcdf)
post_vel_job.uses(maxVelocity_netcdf, link=Link.INPUT)
post_vel_job.uses(maxVelocityImg, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(post_vel_job)

maxVelocity_geojson = File("maxVelocity.geojson")

mvt_job = Job("mvt")
mvt_job.addArguments(maxVelocity_netcdf)
mvt_job.uses(maxVelocity_netcdf, link=Link.INPUT)
mvt_job.uses(maxVelocity_geojson, link=Link.OUTPUT, transfer=True, register=False)
dax.addJob(mvt_job)

#adds file to ldm queue for other to take
#post_mvt_job=Job("")

f = open(daxfile,"w")
dax.writeXML(f)
f.close()
print "Generated dax %s" % daxfile
