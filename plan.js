// If we change:
// Minimatch.prototype.matchOne = function (file, pattern, partial, fi, pi) {
//   var options = this.options
//   fi = fi || 0
//   pi = pi || 0
// ...
//   for (
//       fl = file.length,
//       pl = pattern.length
//       ; (fi < fl) && (pi < pl)
//       ; fi++, pi++) {
// ...
// And the call inside GLOBSTAR loop becomes:
// if (this.matchOne(file, pattern, partial, fr, pr)) {
