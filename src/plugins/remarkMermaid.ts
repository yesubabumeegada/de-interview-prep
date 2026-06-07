/**
 * Custom remark plugin for Mermaid diagram processing.
 * 
 * Transforms fenced code blocks with language "mermaid" into a custom
 * HTML structure that can be rendered client-side by Mermaid.js or
 * displays a fallback if rendering fails.
 * 
 * The plugin wraps mermaid code blocks in a <div class="mermaid-diagram">
 * container with data attributes for client-side rendering and fallback handling.
 */
import { visit } from 'unist-util-visit';
import type { Plugin } from 'unified';
import type { Root, Code } from 'mdast';

const remarkMermaid: Plugin<[], Root> = () => {
  return (tree: Root) => {
    visit(tree, 'code', (node: Code, index, parent) => {
      if (node.lang !== 'mermaid' || !parent || index === undefined) {
        return;
      }

      const source = node.value || '';
      
      // Detect diagram type from the first line.
      // Compare against the lowercased first line for case-insensitive matching.
      const firstLine = source.trim().split('\n')[0]?.toLowerCase() || '';
      let diagramType = 'diagram';
      if (firstLine.startsWith('graph') || firstLine.startsWith('flowchart')) {
        diagramType = 'flowchart';
      } else if (firstLine.startsWith('sequencediagram') || firstLine.startsWith('sequence')) {
        diagramType = 'sequence diagram';
      } else if (firstLine.startsWith('erdiagram') || firstLine.startsWith('er')) {
        diagramType = 'entity-relationship diagram';
      } else if (firstLine.startsWith('classdiagram') || firstLine.startsWith('class')) {
        // Note: compare against lowercased string ('classdiagram', not 'classDiagram')
        diagramType = 'class diagram';
      } else if (firstLine.startsWith('gantt')) {
        diagramType = 'gantt chart';
      } else if (firstLine.startsWith('pie')) {
        diagramType = 'pie chart';
      } else if (firstLine.startsWith('statediagram') || firstLine.startsWith('state')) {
        diagramType = 'state diagram';
      } else if (firstLine.startsWith('gitgraph') || firstLine.startsWith('git')) {
        diagramType = 'git graph';
      } else if (firstLine.startsWith('mindmap')) {
        diagramType = 'mind map';
      } else if (firstLine.startsWith('timeline')) {
        diagramType = 'timeline';
      }

      // Escape HTML entities in the source for safe embedding
      const escapedSource = source
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');

      // Replace the code block with an HTML node containing the mermaid container.
      // The outer div uses width:100%/box-sizing:border-box so the diagram never
      // overflows or requires horizontal scrolling.
      const htmlNode = {
        type: 'html' as const,
        value: `<div class="mermaid-diagram" data-diagram-type="${diagramType}" data-mermaid-source="${escapedSource}" style="width:100%;box-sizing:border-box;">
  <div class="mermaid-render" style="width:100%;box-sizing:border-box;">
    <pre class="mermaid" style="width:100%;box-sizing:border-box;margin:0;">${escapedSource}</pre>
  </div>
  <div class="mermaid-fallback" style="display:none;">
    <p class="mermaid-fallback-type"><strong>Diagram:</strong> ${diagramType}</p>
    <details>
      <summary>View Mermaid source</summary>
      <pre><code class="language-mermaid">${escapedSource}</code></pre>
    </details>
  </div>
</div>`,
      };

      // Replace the code node with our custom HTML
      parent.children.splice(index, 1, htmlNode as any);
    });
  };
};

export default remarkMermaid;
