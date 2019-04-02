#!/usr/bin/perl -w

##################################################################################################
#####WRITTEN BY ERIC LYONS 12/2012 for CASA, UNIVERSITY OF MASSACHUSETTS##########################
##################################################################################################
#  TESTED FUNCTIONALITY:                                                                         #
#                                                                                                #
#  -RECURSIVELY MONITORS DIRECTORIES FOR INCOMING RADAR NETCDF FILES                             #
#                                                                                                #
##################################################################################################

use POSIX qw(setsid);
use File::Copy;
use File::Monitor;
use threads;
use threads::shared;
use lib "/home/ldm/perl";

our $input_data_dir;

our @new_netcdf_files;

##Parse Command Line
&command_line_parse;

##Daemonize process
&daemonize;

##Realtime Mode -- Gets MCC stream
my $file_mon = new threads \&file_monitor;

sleep 100000000;

sub file_monitor {
    
    my $dir_monitor = File::Monitor->new();

    $dir_monitor->watch( {
	name        => "$input_data_dir",
	recurse     => 1,
        callback    => \&new_files,
    } );
    
    $dir_monitor->scan;
    
    for ($i=0; $i < 900000000; $i++) {
	my @changes = $dir_monitor->scan;   
	sleep 5;
    }
    
    sub new_files 
    {
	my ($name, $event, $change) = @_;
	@new_files = $change->files_created;
	my @dels = $change->files_deleted;
	my $pathstr; my $filename;
	#my @new_netcdf_files;
	print "Added: ".join("\nAdded: ", @new_files)."\n" if @new_files;
	foreach $file (@new_files) {
	    ($pathstr, $filename) = $file =~ m|^(.*[/\\])([^/\\]+?)$|;
	    my $sfx = substr($file, -3, 3);
	    if ($sfx eq "cdf") {
            next;
        }
	    sleep 1;
	    my $json_pqins = "pqinsert -f EXP -p " . $filename . " " . $file;
	    system($json_pqins);
	}
    }
}

sub daemonize {
    chdir '/'                 or die "Can't chdir to /: $!";
    open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
    open STDOUT, '>>/dev/null' or die "Can't write to /dev/null: $!";
    open STDERR, '>>/dev/null' or die "Can't write to /dev/null: $!";
    defined(my $pid = fork)   or die "Can't fork: $!";
    exit if $pid;
    setsid                    or die "Can't start a new session: $!";
    umask 0;
}

sub command_line_parse {
    if ($#ARGV < 0) { 
	print "Usage:  dir_mon.pl wf_output_dir\n";
   	exit; 
    }
    $input_data_dir = $ARGV[0];
    
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming netcdf files\n";
    }
    
}
