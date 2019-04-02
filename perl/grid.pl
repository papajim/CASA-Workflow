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

our $inittime = time();
our @new_netcdf_files;

##Parse Command Line
&command_line_parse;

##Daemonize process
&daemonize;

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
	sleep 1;
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
	    if (($sfx eq "cdf") or ($sfx eq ".gz")) { 
		push @new_netcdf_files, $file;
	    }
	}
	my $thistime = time();
	my $timeinterval = $thistime - $inittime;
	#print "Elapsed time: " . $timeinterval . "\n";
	if ($timeinterval > 75) {
	    sleep 1;
	    #print "filelist: " . @new_netcdf_files . "\n";
	    &trigger_pegasus(\@new_netcdf_files);
	    $inittime = $thistime;
	    @new_netcdf_files = ();
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
	print "Usage:  dir_mon.pl netcdf_dir\n";
   	exit; 
    }
    $input_data_dir = $ARGV[0];
    
    my @rdd = split(/ /, $input_data_dir);
    foreach $w (@rdd) {
	print "Will recursively monitor $w for incoming netcdf files\n";
    }
    
}

sub trigger_pegasus {
    my ($flist) = @_;
    my @filelist = @{$flist};
    my $workflow_dir = $ENV{'CASA_WORKFLOW_DIR'};
    system("$workflow_dir/run_casa_wf.sh @filelist");
}
