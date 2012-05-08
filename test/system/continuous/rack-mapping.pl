#!/usr/bin/perl

#An example script for mapping nodes to a rack

%map = (
	"192.168.1.32" => "/rack1",
	"192.168.1.34" => "/rack2",
);

for my $host (@ARGV) {
	if(defined $map{$host}){
		print $map{$host}."\n";
	}else{
		print "/default-rack\n";
	}
}

