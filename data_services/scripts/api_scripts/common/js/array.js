#!/usr/bin/env node
var a={flow: {salesStage: {transition:{ [{ one : 1, two: 2 },  { one : 11, two: 12}, { one : 21, two: 22} ] } } } };
var b=a.flow.salesStage.transition[?(@.one == 11)].two;
console.log("one=11= >", b);
//console.log("one=11= >", a.flow.salesStage.transition[?(@.one == 11)].two);
//console.log("one=11= >", a.flow.salesStage.transition[1].two);
