using Go = import "go.capnp";
$Go.package("influx");
$Go.import("github.com/philhofer/influx");

@0x8cf0c24aaedff7a5;

#'Entry' has 'name' and a list of 'Items'

struct CapEntry {
  name @0 :Text;

  columns @1 :List(Text);

  points @2 :List(PointsT);

}

struct PointsT {
  union {
    int @0 :Int64;
    float @1 :Float64;
    text @2 :Text;
    bool @3 :Bool;
  }
}
