import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import { ProjectProcessorPanel } from "../processors/ProjectProcessorPanel";
import {
  useAssignProjectProcessorMutation,
  useProcessorScriptsQuery,
  useProjectProcessorsQuery,
} from "../processors/hooks";
import { getErrorMessage as getProcessorErrorMessage } from "../processors/utils";
import {
  useAssignProjectRuleSetMutation,
  useDeleteProjectRuleSetAssignmentMutation,
  useProjectRuleSetAssignmentsQuery,
  useRuleSetsQuery,
} from "../rules/hooks";
import { ProjectRuleSetsPanel } from "../rules/ProjectRuleSetsPanel";
import { getErrorMessage as getRuleErrorMessage } from "../rules/utils";
import {
  useCreateProjectMutation,
  useDeleteProjectMutation,
  useProjectsQuery,
  useUpdateProjectMutation,
} from "./hooks";
import { ProjectFormModal } from "./ProjectFormModal";
import { ProjectsTable } from "./ProjectsTable";
import type { Project, ProjectFormValues } from "./types";
import {
  getErrorMessage,
  toCreateProjectPayload,
  toUpdateProjectPayload,
} from "./utils";

export function ProjectsPage() {
  const { message } = App.useApp();
  const projectsQuery = useProjectsQuery();
  const createProjectMutation = useCreateProjectMutation();
  const updateProjectMutation = useUpdateProjectMutation();
  const deleteProjectMutation = useDeleteProjectMutation();
  const ruleSetsQuery = useRuleSetsQuery();
  const processorScriptsQuery = useProcessorScriptsQuery();
  const processorBindingsQuery = useProjectProcessorsQuery();
  const assignProcessorMutation = useAssignProjectProcessorMutation();
  const ruleSets = ruleSetsQuery.data ?? [];
  const processorScripts = processorScriptsQuery.data ?? [];
  const processorBindings = processorBindingsQuery.data ?? [];
  const hasLoadedProjects = projectsQuery.data !== undefined;
  const showInitialError = projectsQuery.isError && !hasLoadedProjects;
  const showRefreshError = projectsQuery.isError && hasLoadedProjects;
  const [modalMode, setModalMode] = useState<"create" | "edit">("create");
  const [editingProject, setEditingProject] = useState<Project | null>(null);
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [deletingProjectId, setDeletingProjectId] = useState<number | null>(null);
  const [updatingRuleSetId, setUpdatingRuleSetId] = useState<number | null>(null);
  const [updatingProcessorProjectId, setUpdatingProcessorProjectId] =
    useState<number | null>(null);
  const projects = projectsQuery.data ?? [];
  const editingProjectId =
    isFormOpen && modalMode === "edit" ? editingProject?.id ?? null : null;
  const assignmentsQuery = useProjectRuleSetAssignmentsQuery(editingProjectId);
  const assignRuleSetMutation = useAssignProjectRuleSetMutation(editingProjectId);
  const deleteAssignmentMutation =
    useDeleteProjectRuleSetAssignmentMutation(editingProjectId);
  const editingProcessorBinding =
    editingProjectId === null
      ? null
      : processorBindings.find((binding) => binding.project_id === editingProjectId) ??
        null;

  const resetFormMutationState = () => {
    createProjectMutation.reset();
    updateProjectMutation.reset();
  };

  const handleCreateClick = () => {
    if (deletingProjectId) {
      return;
    }

    resetFormMutationState();
    setModalMode("create");
    setEditingProject(null);
    setIsFormOpen(true);
  };

  const handleEditClick = (project: Project) => {
    if (deletingProjectId) {
      return;
    }

    resetFormMutationState();
    setModalMode("edit");
    setEditingProject(project);
    setIsFormOpen(true);
  };

  const handleCloseModal = () => {
    if (createProjectMutation.isPending || updateProjectMutation.isPending) {
      return;
    }

    resetFormMutationState();
    setIsFormOpen(false);
    setEditingProject(null);
  };

  const handleSubmit = async (values: ProjectFormValues) => {
    try {
      if (modalMode === "create") {
        await createProjectMutation.mutateAsync(toCreateProjectPayload(values));

        message.success(`Project ${values.name} created`);
      } else if (editingProject) {
        await updateProjectMutation.mutateAsync({
          projectId: editingProject.id,
          payload: toUpdateProjectPayload(values),
        });

        message.success(`Project ${editingProject.name} saved`);
      }

      setIsFormOpen(false);
      setEditingProject(null);
    } catch (error) {
      message.error(
        getErrorMessage(
          error,
          modalMode === "create"
            ? "Failed to create project, please try again later."
            : "Failed to save project, please try again later.",
        ),
      );
      throw error;
    }
  };

  const handleDelete = async (project: Project) => {
    if (deletingProjectId) {
      return;
    }

    setDeletingProjectId(project.id);

    try {
      await deleteProjectMutation.mutateAsync(project.id);
      message.success(`Project ${project.name} deleted`);
    } catch (error) {
      message.error(
        getErrorMessage(error, `Failed to delete project ${project.name}, please try again later.`),
      );
    } finally {
      setDeletingProjectId(null);
    }
  };

  const handleAssignRuleSet = async (ruleSetId: number) => {
    setUpdatingRuleSetId(ruleSetId);
    try {
      await assignRuleSetMutation.mutateAsync({
        rule_set_id: ruleSetId,
        enabled: true,
      });
      message.success("Rule set assigned");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "Failed to assign rule set, please try again later."));
    } finally {
      setUpdatingRuleSetId(null);
    }
  };

  const handleUnassignRuleSet = async (ruleSetId: number) => {
    setUpdatingRuleSetId(ruleSetId);
    try {
      await deleteAssignmentMutation.mutateAsync(ruleSetId);
      message.success("Rule set unassigned");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "Failed to unassign rule set, please try again later."));
    } finally {
      setUpdatingRuleSetId(null);
    }
  };

  const handleAssignProcessor = async (processorScriptId: number) => {
    if (editingProjectId === null) {
      return;
    }

    setUpdatingProcessorProjectId(editingProjectId);
    try {
      await assignProcessorMutation.mutateAsync({
        projectId: editingProjectId,
        payload: {
          processor_script_id: processorScriptId,
          enabled: true,
        },
      });
      message.success("Processor assigned");
    } catch (error) {
      message.error(getProcessorErrorMessage(error, "Failed to assign processor."));
    } finally {
      setUpdatingProcessorProjectId(null);
    }
  };

  const isSubmitting =
    createProjectMutation.isPending || updateProjectMutation.isPending;
  const isDeletePending = deletingProjectId !== null;
  const formError =
    modalMode === "create"
      ? createProjectMutation.error
      : updateProjectMutation.error;

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            Project Management
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            Manage project creation, editing, enabled status, and deletion.
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            disabled={isDeletePending}
            onClick={() => {
              void projectsQuery.refetch();
              void ruleSetsQuery.refetch();
              void processorScriptsQuery.refetch();
              void processorBindingsQuery.refetch();
            }}
            loading={
              projectsQuery.isFetching ||
              ruleSetsQuery.isFetching ||
              processorScriptsQuery.isFetching ||
              processorBindingsQuery.isFetching
            }
          >
            Refresh
          </Button>
          <Button
            type="primary"
            disabled={isDeletePending}
            onClick={handleCreateClick}
          >
            New Project
          </Button>
        </Space>
      </Space>

      {projectsQuery.isLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">Loading projects...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="Failed to load projects"
          subTitle={getErrorMessage(projectsQuery.error)}
          extra={
            <Button type="primary" onClick={() => void projectsQuery.refetch()}>
              Retry
            </Button>
          }
        />
      ) : null}

      {!projectsQuery.isLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`Total ${projects.length} projects`}
          />
          {isDeletePending ? (
            <Alert
              type="info"
              showIcon
              message={`Deleting project #${deletingProjectId}`}
              description="Other edit and delete actions are temporarily disabled while deleting."
            />
          ) : null}
          {isFormOpen && formError ? (
            <Alert
              type="error"
              showIcon
              message={modalMode === "create" ? "Failed to create project" : "Failed to save project"}
              description={getErrorMessage(formError)}
            />
          ) : null}
          {showRefreshError ? (
            <Alert
              type="warning"
              showIcon
              message="Refresh failed, showing last successful data"
              description={getErrorMessage(projectsQuery.error)}
            />
          ) : null}
          {processorScriptsQuery.isError || processorBindingsQuery.isError ? (
            <Alert
              type="warning"
              showIcon
              message="Failed to refresh processor assignment info"
              description={getProcessorErrorMessage(
                processorScriptsQuery.error ?? processorBindingsQuery.error,
              )}
            />
          ) : null}
          <ProjectsTable
            projects={projects}
            processorScripts={processorScripts}
            processorBindings={processorBindings}
            deletingProjectId={deletingProjectId}
            actionsDisabled={isDeletePending}
            onEdit={handleEditClick}
            onDelete={handleDelete}
          />
        </Space>
      ) : null}

      <ProjectFormModal
        open={isFormOpen}
        mode={modalMode}
        project={editingProject}
        confirmLoading={isSubmitting}
        processorSection={
          <ProjectProcessorPanel
            scripts={processorScripts}
            projectName={editingProject?.name}
            projectId={editingProjectId}
            binding={editingProcessorBinding}
            loading={
              processorScriptsQuery.isFetching ||
              processorBindingsQuery.isFetching
            }
            updating={updatingProcessorProjectId === editingProjectId}
            onAssign={handleAssignProcessor}
          />
        }
        ruleSetsSection={
          <ProjectRuleSetsPanel
            ruleSets={ruleSets}
            projectName={editingProject?.name}
            projectId={editingProjectId}
            assignments={assignmentsQuery.data ?? []}
            loading={assignmentsQuery.isFetching || ruleSetsQuery.isFetching}
            updatingRuleSetId={updatingRuleSetId}
            onAssign={handleAssignRuleSet}
            onUnassign={handleUnassignRuleSet}
          />
        }
        onCancel={handleCloseModal}
        onSubmit={handleSubmit}
      />
    </Space>
  );
}
