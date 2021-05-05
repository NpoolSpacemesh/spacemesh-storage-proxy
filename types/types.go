package types

type NewPlotInput struct {
	PlotDir string `json:"dir"`
}

type FinishPlotInput struct {
	PlotFile string `json:"file"`
}

type FailPlotInput = FinishPlotInput
