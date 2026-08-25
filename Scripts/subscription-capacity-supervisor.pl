use strict;
use warnings;
use POSIX qw(setsid WNOHANG);

my $deadline = shift @ARGV;
die "subscription capacity deadline is required\n" unless defined $deadline;
die "subscription capacity command is required\n" unless @ARGV;

my $pid = fork();
die "fork failed: $!\n" unless defined $pid;
if ($pid == 0) {
  setsid() or die "setsid failed: $!\n";
  exec @ARGV or die "exec failed: $!\n";
}

sub terminate_child_group {
  my ($reason) = @_;
  warn "subscription capacity: $reason; terminating process group $pid\n";
  kill "TERM", -$pid;

  my $grace_deadline = time + 2;
  my $child_reaped = 0;
  while (time < $grace_deadline) {
    $child_reaped = 1 if waitpid($pid, WNOHANG) == $pid;
    select undef, undef, undef, 0.05;
  }

  kill "KILL", -$pid;
  waitpid $pid, 0 unless $child_reaped;
}

$SIG{ALRM} = sub {
  terminate_child_group("deadline (${deadline}s) exceeded");
  exit 124;
};
for my $signal (qw(TERM INT HUP)) {
  my $number = $signal eq "TERM" ? 15 : $signal eq "INT" ? 2 : 1;
  $SIG{$signal} = sub {
    alarm 0;
    terminate_child_group("received $signal");
    exit 128 + $number;
  };
}

alarm $deadline;
waitpid $pid, 0;
my $status = $?;
alarm 0;
if ($status & 127) { exit 128 + ($status & 127); }
exit $status >> 8;
