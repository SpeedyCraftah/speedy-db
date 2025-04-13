abi <abi/3.0>,

include <tunables/global>

/usr/lib/speedy-db/production {
  include <abstractions/base>
  include <abstractions/openssl>

  network inet stream,

  # Location of the DB binary - adjust as needed.
  /usr/lib/speedy-db/production mr,

  # Location of the data - adjust as needed (make sure the data directory is owned by the DB user!).
  owner /var/lib/speedy-db/data/** mrw,
}