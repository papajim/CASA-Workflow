#!/usr/bin/env python

import sys
import os
import pwd
import time
from Pegasus.DAX3 import *
from datetime import datetime
from argparse import ArgumentParser

class CASAWorkflow(object):
    def __init__(self, radar_files):
        #self.daxfile = os.path.join(self.outdir, "dax.xml")
        self.daxfile = "casa.dax"
        self.radar_files = radar_files

    def generate_dax(self):
        "Generate a workflow"
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        dax = ADAG("casa_wf-%s" % ts)
        dax.metadata("name", "CASA")
        #USER = pwd.getpwuid(os.getuid())[0]
        #dax.metadata("creator", "%s@%s" % (USER, os.uname()[1]))
        #dax.metadata("created", time.ctime())


        # unzip files if needed
        radar_inputs = []
        for f in self.radar_files:
            if f.endswith(".gz"):
                radar_input = f[:-3]
                radar_inputs.append(radar_input)

                unzip = Job("gunzip")
                unzip.addArguments(f)
                unzip.uses(f, link=Link.INPUT)
                unzip.uses(radar_input, link=Link.OUTPUT, transfer=False, register=False)
                dax.addJob(unzip)
            else:
                radar_inputs.append(f)


        max_velocities = []
        for f in radar_inputs:
            max_velocities.append(File("MaxVelocity_"+f))

        
        # calculate max velocity
        vel_job = Job("um_vel")
        vel_job.addArguments(" ".join(self.radar_files))
        for radar_input in radar_inputs:
            vel_job.uses(radar_input, link=Link.INPUT)
        for max_velocity in max_velocities:
            vel_job.uses(max_velocity, link=Link.OUTPUT, transfer=True, register=False)
        dax.addJob(vel_job)


        max_velocity_images = []
        geojson_files = []
        for max_velocity in max_velocities:
            max_velocity_images.append(File(max_velocity.name+".png")) # change this file name
            geojson_files.append(File(max_velocity.name+".geojson")) # change this file name

        # generate images from max velocities
        colorscale = File("max_wind.png")
        post_vel_files = zip(max_velocities, max_velocity_images)
        for file_set in post_vel_files:
            post_vel_job = Job("merged_netcdf2png")
            post_vel_job.addArguments("-c", colorscale, "-q 235 -z 11.176,38", "-o", file_set[1], file_set[0])
            post_vel_job.uses(file_set[0], link=Link.INPUT)
            post_vel_job.uses(file_set[1], link=Link.OUTPUT, transfer=True, register=False)
            dax.addJob(post_vel_job)

        # generate geojson files from max velocities
        mvt_files = zip(max_velocities, geojson_files)
        for file_set in mvt_files:
            mvt_job = Job("mvt")
            mvt_job.addArguments(file_set[0])
            mvt_job.uses(file_set[0], link=Link.INPUT)
            mvt_job.uses(file_set[1], link=Link.OUTPUT, transfer=True, register=False)
            dax.addJob(mvt_job)

        #adds file to ldm queue for other to take
        #post_mvt_job=Job("")

        # Write the DAX file
        dax.writeXMLFile(self.daxfile)
        
    def generate_workflow(self):
        # Generate dax
        self.generate_dax()

if __name__ == '__main__':
    parser = ArgumentParser(description="CASA Workflow")
    parser.add_argument("-f", "--files", metavar="INPUT_FILES", type=str, nargs="+", help="Configuration File", required=True)
    #parser.add_argument("-o", "--outdir", metavar="OUTPUT_LOCATION", type=str, help="Workflow Directory", required=True)

    args = parser.parse_args()

    #if os.path.isdir(args.outdir):
    #    raise Exception("Directory exists: %s" % args.outdir)

    # Create the output directory
    #outdir = os.path.abspath(args.outdir)
    #os.makedirs(outdir)


    workflow = CASAWorkflow(args.files)
    workflow.generate_workflow()
