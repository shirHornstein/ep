// Distributed Query Processing & Execution Framework. Short for (and
// pronounced) Epsilon, ep is designed to make it easy to construct complex
// query engines and data processing pipelines that are distributed across a
// cluster of nodes.
//
// Runners and Data
//
// The basic design of ep is to implement and compose multiple Runner objects
// where each performs a single transformation on its input datasets and
// produces the modified datasets in return. These transformations can include
// anything from mapping, reduction, filtering, etc.
//
// The input and output of the Runners are channels of Datasets. Each Dataset is
// composed of several typed Data instance, each can be thought of as
// representing a column of data of the same type, thus the whole Dataset
// represents a batch of rows in a table. These batches are streamed via
// channels, making each Runner a long-lived function that consumes its input,
// transforms it and produces its output until the input channel is closed.
//
// Manually executing the Run function on these runners is a bit cumbersome, as
// care must be taken when constructing the input and output and handling the
// errors, as you can see in the examples below. However, Runners should rarely
// be executed manually - they're designed to be composed together into just a
// single top-level Runner that actually needs to be run. Ep already includes
// some composition implementations like Pipeline, Project, Union, etc. as well
// as utility Runners that can be used for data exchange or otherwise. If you
// encounter a use-case where a Runner must be composed in a new way, consider
// if there's a generic version of it that can be included in this project for
// re-use.
//
// Finally - the actual Data instances are left for user-space implementations.
// Please review the example code below, or the Null type for reference. The
// latter is part of the ep project, and provides the ability to transmit null-
// data around without the overhead of passing around interfaces or pointers.
//
// Registeries
//
// In order to support modular design, where Runners and Types are spread across
// several different projects, ep includes global registeries that can be used
// to share access to these declared structures. These are available through
// the global `Runners` and `Types` variables, and they share the same generic
// interface:
//
//      Runners.Register(k interface{}, r Runners) Runners
//      Runners.Get(k interface{}) []Runner
//
//      Types.Register(k interface{}, t Type) Types
//      Types.Get(k interface{}) []Type
//
// It's comparable to a global key-value registry of runners and types with
// one caveat - if the key is a struct, it's first converted into a string by
// reflecting its full type name and path. This effectively means that
// registering several runners using different instances of the same struct will
// land on the same key. This is useful for planning, where we want to match
// based on instances of that struct. See Planning below.
//
// Planning
//
// Planning is the process of constructing Runners based on some configuration
// or user-input, like an SQL query or parsed AST. It's completely optional, but
// helpful in cases where the produced Runners can vary wildly and frequently.
// Planning in Ep is a two step process:
//
// First, the Runners must be globally registered using the Runners registry,
// for an arbitrary key argument:
//
//      ep.Runners.Register(ast.SelectStmt{}, &SelectRunner{})
//      ep.Runners.Register("SUM", &SumRunner{})
//
// The key can be anything, but if it's a struct, it's first converted into a
// string via reflection using the full type name and path (see Registeries
// above). Then, using the same key (or new instances of the same key) you can
// generate Runners of it via the .Plan() function:
//
//      ep.Plan(ctx, ast.SelectStmt{}) // returns &SelectRunner{}
//      ep.Plan(ctx, "SUM") // returns &SumRunner{}
//
// If the returned Runner implements RunnerPlan, it's first called with the same
// arguments, in order to allow it to plan itself. In case of a planning error,
// Plan will fallthrough to the next Runner of the same key, thus implementing a
// kind of middlewares systems where atleast one RunnerPlan must succeed. This
// allows an opportunistic design where several runners bind to the same node,
// each planning it differently - if they can.
package ep

import (
    "encoding/gob"
)

func registerGob(es ...interface{}) bool {
    for _, e := range es {
        gob.Register(e)
    }
    return true
}
