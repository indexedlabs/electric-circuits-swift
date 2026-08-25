use strict;
use warnings;
use POSIX qw(SIG_BLOCK SIG_UNBLOCK SIGINT SIGHUP SIGTERM WNOHANG setsid sigprocmask);

my $deadline = shift @ARGV;
die "subscription capacity deadline is required\n" unless defined $deadline;
die "subscription capacity command is required\n" unless @ARGV;

my $cancellation_signals = POSIX::SigSet->new(SIGTERM, SIGINT, SIGHUP);
sigprocmask(SIG_BLOCK, $cancellation_signals) or die "failed to block cancellation signals: $!\n";

pipe(my $ready_read, my $ready_write) or die "readiness pipe failed: $!\n";
pipe(my $go_read, my $go_write) or die "go pipe failed: $!\n";
my $pid = fork();
die "fork failed: $!\n" unless defined $pid;
if ($pid == 0) {
  close $ready_read or die "readiness pipe close failed: $!\n";
  close $go_write or die "go pipe close failed: $!\n";
  if (my $pid_file = $ENV{CAPACITY_SUPERVISOR_TEST_PRE_READY_PID_FILE}) {
    open my $fh, ">", $pid_file or die "pre-ready pid file failed: $!\n";
    print {$fh} "$$\n" or die "pre-ready pid write failed: $!\n";
    close $fh or die "pre-ready pid close failed: $!\n";
  }
  if (my $gate = $ENV{CAPACITY_SUPERVISOR_TEST_PRE_READY_GATE}) {
    select undef, undef, undef, 0.01 until -e $gate;
  }
  setsid() or die "setsid failed: $!\n";
  print {$ready_write} "R" or die "readiness acknowledgement failed: $!\n";
  close $ready_write or die "readiness acknowledgement close failed: $!\n";
  my $go = "";
  my $read = sysread($go_read, $go, 1);
  close $go_read or die "go pipe close failed: $!\n";
  exit 127 if !defined $read || $read != 1 || $go ne "G";
  $SIG{TERM} = "DEFAULT";
  $SIG{INT} = "DEFAULT";
  $SIG{HUP} = "DEFAULT";
  sigprocmask(SIG_UNBLOCK, $cancellation_signals)
    or die "failed to unblock cancellation signals: $!\n";
  exec @ARGV or die "exec failed: $!\n";
}
close $ready_write or die "readiness pipe close failed: $!\n";
close $go_read or die "go pipe close failed: $!\n";

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

sub terminate_unready_child {
  my ($reason) = @_;
  warn "subscription capacity: $reason; terminating unready child $pid\n";
  kill "TERM", $pid;
  my $grace_deadline = time + 2;
  while (time < $grace_deadline) {
    return if waitpid($pid, WNOHANG) == $pid;
    select undef, undef, undef, 0.05;
  }
  kill "KILL", $pid;
  waitpid $pid, 0;
}

my $group_ready = 0;

$SIG{ALRM} = sub {
  $group_ready
    ? terminate_child_group("deadline (${deadline}s) exceeded")
    : terminate_unready_child("deadline (${deadline}s) exceeded");
  exit 124;
};
for my $signal (qw(TERM INT HUP)) {
  my $number = $signal eq "TERM" ? 15 : $signal eq "INT" ? 2 : 1;
  $SIG{$signal} = sub {
    alarm 0;
    $group_ready
      ? terminate_child_group("received $signal")
      : terminate_unready_child("received $signal");
    exit 128 + $number;
  };
}

alarm $deadline;
my $acknowledgement = "";
my $read = sysread($ready_read, $acknowledgement, 1);
close $ready_read or die "readiness pipe close failed: $!\n";
if (!defined $read || $read != 1 || $acknowledgement ne "R") {
  close $go_write;
  terminate_unready_child("child exited before group readiness");
  sigprocmask(SIG_UNBLOCK, $cancellation_signals)
    or die "failed to unblock cancellation signals: $!\n";
  exit 127;
}
if (my $ready_observed = $ENV{CAPACITY_SUPERVISOR_TEST_READY_OBSERVED_FILE}) {
  open my $fh, ">", $ready_observed or die "ready-observed file failed: $!\n";
  print {$fh} "ready\n" or die "ready-observed write failed: $!\n";
  close $fh or die "ready-observed close failed: $!\n";
}
if (my $gate = $ENV{CAPACITY_SUPERVISOR_TEST_PARENT_READY_GATE}) {
  select undef, undef, undef, 0.01 until -e $gate;
}
$group_ready = 1;
my $go_sent = print {$go_write} "G";
my $go_closed = close $go_write;
if (!$go_sent || !$go_closed) {
  terminate_child_group("parent could not authorize child execution");
  sigprocmask(SIG_UNBLOCK, $cancellation_signals)
    or die "failed to unblock cancellation signals: $!\n";
  exit 127;
}
sigprocmask(SIG_UNBLOCK, $cancellation_signals)
  or die "failed to unblock cancellation signals: $!\n";
waitpid $pid, 0;
my $status = $?;
alarm 0;
if ($status & 127) { exit 128 + ($status & 127); }
exit $status >> 8;
