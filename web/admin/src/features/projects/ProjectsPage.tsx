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
  const projects = projectsQuery.data ?? [];
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
        message.success(`项目 ${values.name} 创建成功`);
      } else if (editingProject) {
        await updateProjectMutation.mutateAsync({
          projectId: editingProject.id,
          payload: toUpdateProjectPayload(values),
        });
        message.success(`项目 ${editingProject.name} 保存成功`);
      }

      setIsFormOpen(false);
      setEditingProject(null);
    } catch (error) {
      message.error(
        getErrorMessage(
          error,
          modalMode === "create"
            ? "创建项目失败，请稍后重试。"
            : "保存项目失败，请稍后重试。",
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
      message.success(`项目 ${project.name} 删除成功`);
    } catch (error) {
      message.error(
        getErrorMessage(error, `删除项目 ${project.name} 失败，请稍后重试。`),
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
      message.success("规则集绑定成功");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "绑定规则集失败，请稍后重试。"));
    } finally {
      setUpdatingRuleSetId(null);
    }
  };

  const handleUnassignRuleSet = async (ruleSetId: number) => {
    setUpdatingRuleSetId(ruleSetId);
    try {
      await deleteAssignmentMutation.mutateAsync(ruleSetId);
      message.success("规则集解绑成功");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "解绑规则集失败，请稍后重试。"));
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
      message.success("Processor 绑定成功");
    } catch (error) {
      message.error(getProcessorErrorMessage(error, "绑定 Processor 失败。"));
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
            项目管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            管理项目的创建、编辑、启停状态和删除操作。
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
            刷新
          </Button>
          <Button
            type="primary"
            disabled={isDeletePending}
            onClick={handleCreateClick}
          >
            新建项目
          </Button>
        </Space>
      </Space>

      {projectsQuery.isLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">正在加载项目列表...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="项目列表加载失败"
          subTitle={getErrorMessage(projectsQuery.error)}
          extra={
            <Button type="primary" onClick={() => void projectsQuery.refetch()}>
              重试
            </Button>
          }
        />
      ) : null}

      {!projectsQuery.isLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`共 ${projects.length} 个项目`}
          />
          {isDeletePending ? (
            <Alert
              type="info"
              showIcon
              message={`正在删除项目 #${deletingProjectId}`}
              description="删除完成前，已临时禁用其他编辑和删除操作。"
            />
          ) : null}
          {isFormOpen && formError ? (
            <Alert
              type="error"
              showIcon
              message={modalMode === "create" ? "创建项目失败" : "保存项目失败"}
              description={getErrorMessage(formError)}
            />
          ) : null}
          {showRefreshError ? (
            <Alert
              type="warning"
              showIcon
              message="刷新失败，当前展示的是上次成功加载的数据"
              description={getErrorMessage(projectsQuery.error)}
            />
          ) : null}
          {processorScriptsQuery.isError || processorBindingsQuery.isError ? (
            <Alert
              type="warning"
              showIcon
              message="Processor 绑定信息刷新失败"
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
